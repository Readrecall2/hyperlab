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
ACQUIRED_REMOTE_ARCHIVE_SHA256 = (
    "6e7c094dfb45d901f2f1b77bde8e53958075e9e67349e5cb9f4d125b1c031ea8"
)
ACQUIRED_REMOTE_FILE_COUNT = 20
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


def render_windows_fetch(
    *,
    expected_archive_sha256: str | None = None,
    expected_file_count: int | None = None,
) -> str:
    if (expected_archive_sha256 is None) != (expected_file_count is None):
        raise BuildError("acquired archive SHA-256 and file count must be supplied together")
    if expected_archive_sha256 is not None and re.fullmatch(
        r"[0-9a-f]{64}", expected_archive_sha256
    ) is None:
        raise BuildError("expected forensic archive SHA-256 is invalid")
    if expected_file_count is not None and expected_file_count <= 0:
        raise BuildError("expected forensic file count must be positive")
    acquired_archive_check = (
        ""
        if expected_archive_sha256 is None
        else f"if ($ArchiveHash -cne '{expected_archive_sha256}') {{ throw 'Forensic archive differs from the acquired F evidence.' }}\n"
    )
    acquired_count_check = (
        ""
        if expected_file_count is None
        else f"if (@($Inventory.files).Count -ne {expected_file_count}) {{ throw 'Forensic inventory file count diverged from the acquired F evidence.' }}\n"
    )
    return f"""# Lieu: Windows PowerShell local. Durée attendue: <2 min; maximum: 10 min.
# Prompts: SSH host-key/password possibles. Ctrl+C interrompt uniquement la copie.
# Signal terminal: PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED.
$ErrorActionPreference = 'Stop'
$SshTarget = $env:HYPERLAB_PM_SSH_TARGET
if ([string]::IsNullOrWhiteSpace($SshTarget) -or $SshTarget -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
$SshKeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($SshKeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY.' }}
if (-not [IO.Path]::IsPathRooted($SshKeyRaw)) {{ throw 'SSH key path must be absolute.' }}
$SshKey = (Resolve-Path -LiteralPath $SshKeyRaw).Path
if (-not (Test-Path -LiteralPath $SshKey -PathType Leaf)) {{ throw 'SSH key is not a regular file.' }}
$LocalRootRaw = $env:HYPERLAB_PM_FORENSIC_LOCAL_ROOT
if ([string]::IsNullOrWhiteSpace($LocalRootRaw)) {{ throw 'Set HYPERLAB_PM_FORENSIC_LOCAL_ROOT to a new local directory.' }}
if (-not [IO.Path]::IsPathRooted($LocalRootRaw)) {{ throw 'Local forensic root must be absolute.' }}
$LocalRoot = [IO.Path]::GetFullPath($LocalRootRaw)
if (Test-Path -LiteralPath $LocalRoot) {{ throw 'Local forensic root must be new.' }}
New-Item -ItemType Directory -Path $LocalRoot | Out-Null
$RemoteRoot = '{FORENSIC_ROOT}'
$Names = @('forensic-scope.json','forensic-inventory.json','forensic-inventory.json.sha256','receipt-auth-forensic.tar','receipt-auth-forensic.tar.sha256')
$MaximumBytes = @{{
    'forensic-scope.json' = 4194304
    'forensic-inventory.json' = 4194304
    'forensic-inventory.json.sha256' = 256
    'receipt-auth-forensic.tar' = 41943040
    'receipt-auth-forensic.tar.sha256' = 256
}}
function Assert-RegularBounded {{
    param([string] $Path, [int64] $Maximum)
    $Item = Get-Item -LiteralPath $Path -Force
    if ($Item.PSIsContainer -or (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) -or $Item.Length -gt $Maximum) {{ throw "Transferred file is special or oversized: $Path" }}
}}
foreach ($Name in $Names) {{
    scp -i $SshKey -- "${{SshTarget}}:${{RemoteRoot}}/${{Name}}" (Join-Path $LocalRoot $Name)
    if ($LASTEXITCODE -ne 0) {{ throw "Forensic transfer failed: $Name" }}
    Assert-RegularBounded (Join-Path $LocalRoot $Name) $MaximumBytes[$Name]
}}
function Get-Sha256Hex {{
    param([string] $Path)
    $Before = Get-Item -LiteralPath $Path -Force
    $Algorithm = [Security.Cryptography.SHA256]::Create()
    $Stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
    try {{
        $Digest = $Algorithm.ComputeHash($Stream)
    }} finally {{
        $Stream.Dispose()
        $Algorithm.Dispose()
    }}
    $After = Get-Item -LiteralPath $Path -Force
    if ($Before.Length -ne $After.Length -or $Before.LastWriteTimeUtc.Ticks -ne $After.LastWriteTimeUtc.Ticks) {{ throw "File identity changed during hashing: $Path" }}
    return ([BitConverter]::ToString($Digest)).Replace('-', '').ToLowerInvariant()
}}
function Assert-Pin {{
    param([string] $PinName, [string] $TargetName)
    if ([IO.Path]::GetFileName($PinName) -cne $PinName -or [IO.Path]::GetFileName($TargetName) -cne $TargetName) {{ throw "Unsafe pin basename: $PinName" }}
    if ($Names -cnotcontains $PinName -or $Names -cnotcontains $TargetName) {{ throw "Pin target is outside the transfer allowlist: $PinName" }}
    $PinPath = Join-Path $LocalRoot $PinName
    $PinBytes = [IO.File]::ReadAllBytes($PinPath)
    foreach ($Byte in $PinBytes) {{ if ($Byte -gt 127) {{ throw "Pin is not ASCII: $PinName" }} }}
    $Raw = [Text.Encoding]::ASCII.GetString($PinBytes)
    if (-not $Raw.EndsWith("`n")) {{ throw "Pin lacks its canonical LF terminator: $PinName" }}
    $Line = $Raw.Substring(0, $Raw.Length - 1)
    if ($Line.IndexOf("`r") -ge 0 -or $Line.IndexOf("`n") -ge 0) {{ throw "Pin contains extra lines: $PinName" }}
    if ($Line.Length -ne (66 + $TargetName.Length) -or $Line.Substring(64, 2) -cne '  ') {{ throw "Malformed pin layout: $PinName" }}
    $ExpectedHash = $Line.Substring(0, 64)
    foreach ($Character in $ExpectedHash.ToCharArray()) {{
        if ('0123456789abcdef'.IndexOf([char] $Character) -lt 0) {{ throw "Malformed lowercase SHA-256: $PinName" }}
    }}
    $PinnedName = $Line.Substring(66)
    if ($PinnedName -cne $TargetName) {{ throw "Pin basename diverged: $PinName" }}
    if ((Get-Sha256Hex (Join-Path $LocalRoot $TargetName)) -cne $ExpectedHash) {{ throw "SHA-256 diverged: $TargetName" }}
    return $ExpectedHash
}}
$null = Assert-Pin 'forensic-inventory.json.sha256' 'forensic-inventory.json'
$ArchiveHash = Assert-Pin 'receipt-auth-forensic.tar.sha256' 'receipt-auth-forensic.tar'
{acquired_archive_check}$Inventory = Get-Content -LiteralPath (Join-Path $LocalRoot 'forensic-inventory.json') -Raw | ConvertFrom-Json
if ($Inventory.source_commit -cne '{FAILED_SOURCE_COMMIT}' -or $Inventory.failed_campaign_root -cne '{CAMPAIGN_ROOT}') {{ throw 'Forensic identity diverged.' }}
{acquired_count_check}$ScopeEntry = @($Inventory.files | Where-Object {{ $_.path -ceq 'forensic-scope.json' }})
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
if (-not [IO.Path]::IsPathRooted($BundleRootRaw)) {{ throw 'Local forensic root must be absolute.' }}
$BundleRoot = [IO.Path]::GetFullPath($BundleRootRaw)
if (-not (Test-Path -LiteralPath $BundleRoot -PathType Container)) {{ throw 'Local forensic root is absent.' }}
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


def render_windows_followup_readme(tool_commit: str, tool_sha256: str) -> str:
    return f"""# Prediction Markets receipt-auth Windows follow-up

Le bloc F est déjà acquis et ne doit pas être rejoué. Ce pack contient uniquement
le nouveau bloc G PowerShell 5.1, le bloc H offline et leur outil authentifié. G
relit les cinq fichiers du forensic root existant vers une nouvelle racine locale.

- frontière: `{BOUNDARY}`
- failed source commit: `{FAILED_SOURCE_COMMIT}`
- forensic tool commit: `{tool_commit}`
- embedded tool SHA-256: `{tool_sha256}`
- remote forensic root read-only: `{FORENSIC_ROOT}`
- acquired archive SHA-256: `{ACQUIRED_REMOTE_ARCHIVE_SHA256}`
- acquired forensic file count: `{ACQUIRED_REMOTE_FILE_COUNT}`
- terminal fetch: `PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_FETCHED`
- terminal diagnostic: `PREDICTION_MARKETS_RECEIPT_AUTH_DIVERGENCE_IDENTIFIED`
"""


def build_pack(
    *,
    repo_root: Path,
    output_root: Path,
    tool_commit: str,
    windows_followup: bool = False,
) -> dict[str, object]:
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
    fetch = (
        render_windows_fetch(
            expected_archive_sha256=ACQUIRED_REMOTE_ARCHIVE_SHA256,
            expected_file_count=ACQUIRED_REMOTE_FILE_COUNT,
        )
        if windows_followup
        else render_windows_fetch()
    )
    files = {
        "operator/G-windows-fetch-receipt-auth-forensic.ps1": fetch.encode(),
        "operator/H-windows-offline-diagnose.ps1": render_windows_diagnose().encode(),
        "tools/receipt_auth_forensics.py": tool_raw,
    }
    if windows_followup:
        readme = render_windows_followup_readme(
            tool_commit,
            sha256_bytes(tool_raw),
        ).encode()
    else:
        files["operator/F-tabby-export-receipt-auth-forensic.sh"] = render_tabby_export(
            tool_raw
        ).encode()
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
        "scope": (
            "WINDOWS_FETCH_DIAG_ONLY_REMOTE_FORENSIC_REUSE"
            if windows_followup
            else "FORENSIC_EXPORT_FETCH_DIAG"
        ),
        "tool_commit": tool_commit,
    }
    if windows_followup:
        inventory_body.update(
            {
                "acquired_remote_archive_sha256": ACQUIRED_REMOTE_ARCHIVE_SHA256,
                "acquired_remote_file_count": ACQUIRED_REMOTE_FILE_COUNT,
            }
        )
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
    parser.add_argument("--windows-followup", action="store_true")
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    result = build_pack(
        repo_root=arguments.repo_root,
        output_root=arguments.output_root,
        tool_commit=arguments.tool_commit,
        windows_followup=arguments.windows_followup,
    )
    print(canonical_json_bytes(result).decode())
    print("PREDICTION_MARKETS_RECEIPT_AUTH_FORENSIC_TOOL_PACK_GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
