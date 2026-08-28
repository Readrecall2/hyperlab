from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn
from uuid import uuid4

BOUNDARY = "PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY"
PROOF_KIND = "SUPERSEDED_RUNTIME_COMPATIBILITY_PROOF"
EXPECTED_BRANCH = "codex/prediction-markets-v3-independent-audit"
TARGET_COMMIT = "bcb5280f87393992e2aa4528188009186cd8bdc3"
TARGET_SLUG = "pm-20260828t024827z-bcb5280f"
TARGET_INVENTORY_SHA256 = (
    "573db1e313459d8b153cc6790fd733bd790898eeacaaade4125f48c28a3edf53"
)
ADAPTER_ID = "prediction-markets-bcb5280f-runtime-v1"
BUNDLE_NAME = "hyperlab-superseded-runtime-compatibility.bundle"
REPORT_NAME = "superseded-runtime-compatibility-proof-report.json"
RUNTIME_REPORT_NAME = "superseded-runtime-compatibility.json"
OUTPUT_INVENTORY_NAME = "superseded-runtime-compatibility-output-inventory.json"
_RUN_SLUG = re.compile(r"^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SCRIPT_RELATIVE = "ops/prediction_markets_launch_v1/superseded_compat_proof.py"
_SOURCE_FILES = (
    "launch_pack.py",
    "superseded_compat_proof.py",
)
_TRANSFER_PATHS = (
    BUNDLE_NAME,
    "README.md",
    "source-inventory.json",
    "scripts/launch_pack.py",
    "scripts/superseded_compat_proof.py",
    "operator/A1-windows-transfer-authenticated.ps1",
    "operator/B1-tabby-superseded-readonly-proof.sh",
    "operator/C1-windows-retrieve-authenticate.ps1",
)
_OUTPUT_FILES = (
    RUNTIME_REPORT_NAME,
    REPORT_NAME,
    f"{REPORT_NAME}.sha256",
)


class CompatibilityProofError(RuntimeError):
    """Fail-closed superseded-runtime compatibility proof error."""


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


def _safe_bytes(path: Path, *, maximum: int = 128 * 1024 * 1024) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise CompatibilityProofError(f"required file is unreadable: {path}") from error
    if path.is_symlink() or not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
        raise CompatibilityProofError(f"required file is unsafe: {path}")
    raw = path.read_bytes()
    after = path.lstat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) or len(raw) != before.st_size:
        raise CompatibilityProofError(f"file changed during authentication: {path}")
    return raw


def _object(path: Path) -> dict[str, Any]:
    raw = _safe_bytes(path)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CompatibilityProofError(f"invalid JSON object: {path}") from error
    if not isinstance(value, dict):
        raise CompatibilityProofError(f"JSON root is not an object: {path}")
    return value


def _canonical_object(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = _safe_bytes(path)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CompatibilityProofError(f"invalid canonical JSON: {path}") from error
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise CompatibilityProofError(f"JSON is not canonical LF-terminated: {path}")
    return value, raw


def _write_new(path: Path, payload: bytes, *, executable: bool = False) -> None:
    if path.exists() or path.is_symlink():
        raise CompatibilityProofError(f"output path already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o700 if executable else 0o600,
    )
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _git(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        capture_output=True,
        check=False,
        text=True,
        timeout=60,
    )
    if completed.returncode != 0:
        raise CompatibilityProofError(completed.stderr.strip() or "Git command failed")
    return completed.stdout.strip()


def _git_bytes(repo_root: Path, commit: str, relative: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "show", f"{commit}:{relative}"],
        capture_output=True,
        check=False,
        timeout=60,
    )
    if completed.returncode != 0:
        raise CompatibilityProofError(
            completed.stderr.decode("utf-8", errors="replace").strip()
            or f"Git blob is unavailable: {relative}"
        )
    return completed.stdout


def _source_inventory(repo_root: Path, commit: str) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for line in _git(repo_root, "ls-tree", "-r", "--long", commit).splitlines():
        metadata, separator, relative = line.partition("\t")
        fields = metadata.split()
        if not separator or len(fields) != 4 or fields[1] != "blob" or not fields[3].isdigit():
            raise CompatibilityProofError("Git source inventory line is malformed")
        rows.append(
            {
                "blob_sha1": fields[2],
                "mode": fields[0],
                "path": relative,
                "size": int(fields[3]),
            }
        )
    body: dict[str, object] = {
        "boundary": BOUNDARY,
        "commit": commit,
        "files": rows,
        "schema_version": 1,
    }
    return {**body, "inventory_sha256": sha256_bytes(canonical_json_bytes(body))}


def _service_names(slug: str) -> dict[str, str]:
    suffix = slug.removeprefix("pm-")
    return {
        venue: f"hyperlab-pm-{suffix}-{venue}.service"
        for venue in ("polymarket", "kalshi", "dashboard")
    }


def _probe_names(slug: str) -> dict[str, str]:
    suffix = slug.removeprefix("pm-")
    return {
        venue: f"hyperlab-pm-{suffix}-{venue}-namespace-probe.service"
        for venue in ("polymarket", "kalshi")
    }


def _target_contract() -> dict[str, object]:
    base = "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
    return {
        "campaign_root": f"{base}/campaigns/{TARGET_SLUG}",
        "dashboard_port": 18081,
        "incoming_root": (
            "/home/hyperlab/hyperlab-prediction-markets/incoming/" + TARGET_SLUG
        ),
        "namespace_probe_services": _probe_names(TARGET_SLUG),
        "run_slug": TARGET_SLUG,
        "services": _service_names(TARGET_SLUG),
        "source_commit": TARGET_COMMIT,
        "source_root": f"{base}/sources/{TARGET_SLUG}",
    }


def _render_readme(handoff: Mapping[str, object]) -> str:
    return f"""# SUPERSEDED_RUNTIME_COMPATIBILITY_PROOF

Ce pack prouve en lecture seule la compatibilité entre le vérificateur candidat
`{handoff['source_commit']}` et la campagne historique active `{TARGET_SLUG}`.
Il ne contient aucun cutover, install, disarm, stop/start/restart/enable/disable,
sudo, bind de port, collecteur ou mutation de campagne. Le clone candidat et les
rapports utilisent uniquement les nouvelles racines de preuve ci-dessous.

- incoming : `{handoff['incoming_root']}`
- source candidat : `{handoff['source_root']}`
- campagne candidate réservée mais jamais créée : `{handoff['campaign_root']}`
- source historique target : `{_target_contract()['source_root']}`
- adaptateur : `{ADAPTER_ID}`

L'alias multiprocessing `__mp_main__` n'est admis que s'il désigne exactement
`__main__`, c'est-à-dire ce vérificateur candidat réauthentifié contre son blob
Git; le rapport et C1 lient explicitement cette classe `candidate_tool`.

Ordre humain : A1 Windows, B1 Tabby/VPS, C1 Windows. Le signal terminal B1 est
`PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER`. Ctrl+C pendant
B1 n'affecte que le clone/rapport de preuve et laisse la campagne historique
active. Aucun bloc final A/B/C/D/E n'est fourni par ce pack.
"""


def _render_a1(handoff: Mapping[str, object]) -> str:
    incoming = str(handoff["incoming_root"])
    return f"""# Lieu: Windows PowerShell local. Durée attendue: 2-6 min; maximum: 20 min.
# Prompts: SSH host-key/password possibles. Ctrl+C interrompt uniquement le transfert.
# Monitoring: aucun service n'est touché. Signal: PREDICTION_SUPERSEDED_COMPATIBILITY_TRANSFER_GREEN.
$ErrorActionPreference = 'Stop'
$OperatorRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$PackRoot = (Resolve-Path -LiteralPath (Join-Path $OperatorRoot '..')).Path
$Python = (Get-Command python).Source
& $Python -I (Join-Path $PackRoot 'scripts/superseded_compat_proof.py') verify-input --proof-root $PackRoot
if ($LASTEXITCODE -ne 0) {{ throw 'Local compatibility proof pack authentication failed.' }}
$Target = $env:HYPERLAB_PM_SSH_TARGET
$KeyRaw = $env:HYPERLAB_PM_SSH_KEY
if ([string]::IsNullOrWhiteSpace($Target) -or $Target -notmatch '^[a-z0-9._-]+@[a-z0-9.-]+$') {{ throw 'Set HYPERLAB_PM_SSH_TARGET to user@host.' }}
if ([string]::IsNullOrWhiteSpace($KeyRaw)) {{ throw 'Set HYPERLAB_PM_SSH_KEY.' }}
$Key = (Resolve-Path -LiteralPath $KeyRaw).Path
$Incoming = '{incoming}'
ssh -i $Key $Target "test ! -e '$Incoming' && install -d -m 0700 '$Incoming'"
if ($LASTEXITCODE -ne 0) {{ throw 'Unique compatibility incoming root creation failed.' }}
Get-ChildItem -LiteralPath $PackRoot -Force | ForEach-Object {{
    scp -i $Key -r -- $_.FullName "${{Target}}:${{Incoming}}/"
    if ($LASTEXITCODE -ne 0) {{ throw "Compatibility transfer failed: $($_.Name)" }}
}}
$Remote = "python3.12 -I '$Incoming/scripts/superseded_compat_proof.py' verify-input --proof-root '$Incoming' --require-remote-layout"
ssh -i $Key $Target $Remote
if ($LASTEXITCODE -ne 0) {{ throw 'Remote compatibility proof pack authentication failed.' }}
Write-Output 'PREDICTION_SUPERSEDED_COMPATIBILITY_TRANSFER_GREEN'
"""


def _render_b1(handoff: Mapping[str, object]) -> str:
    incoming = str(handoff["incoming_root"])
    source = str(handoff["source_root"])
    commit = str(handoff["source_commit"])
    return f"""#!/usr/bin/env bash
# Lieu: Tabby/VPS Bash sous hyperlab. Durée attendue: 3-10 min; maximum: 20 min.
# Prompts: aucun, aucun sudo. Monitoring: systemd/port/process sont lus seulement.
# Ctrl+C arrête clone/import/rapport de preuve; la campagne historique reste active.
# Signal terminal exact: PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER.
set -Eeuo pipefail
umask 077
INCOMING_ROOT='{incoming}'
SOURCE_ROOT='{source}'
[[ $(id -un) == hyperlab && $HOME == /home/hyperlab ]] || {{ printf 'PREDICTION_SUPERSEDED_COMPATIBILITY_REFUSED:user_or_home\n' >&2; exit 4; }}
python3.12 -I "$INCOMING_ROOT/scripts/superseded_compat_proof.py" verify-input --proof-root "$INCOMING_ROOT" --require-remote-layout
[[ ! -e "$SOURCE_ROOT" && ! -L "$SOURCE_ROOT" ]] || {{ printf 'PREDICTION_SUPERSEDED_COMPATIBILITY_REFUSED:source_root_not_new\n' >&2; exit 4; }}
git clone --no-checkout "$INCOMING_ROOT/{BUNDLE_NAME}" "$SOURCE_ROOT"
git -C "$SOURCE_ROOT" checkout --detach '{commit}'
python3.12 -I "$INCOMING_ROOT/scripts/superseded_compat_proof.py" verify-source --proof-root "$INCOMING_ROOT" --source-root "$SOURCE_ROOT"
OUTPUT="$INCOMING_ROOT/superseded-runtime-compatibility-verify-old.stdout"
[[ ! -e "$OUTPUT" && ! -L "$OUTPUT" ]] || {{ printf 'PREDICTION_SUPERSEDED_COMPATIBILITY_REFUSED:output_exists\n' >&2; exit 4; }}
timeout --signal=TERM --kill-after=5s 300s \\
  bash "$SOURCE_ROOT/ops/prediction_markets_launch_v1/cutover.sh" verify-old "$INCOMING_ROOT/handoff.json" > "$OUTPUT"
python3.12 -I "$INCOMING_ROOT/scripts/superseded_compat_proof.py" finalize-output --proof-root "$INCOMING_ROOT" --cutover-output "$OUTPUT"
printf 'PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER\n'
"""


def _render_c1(handoff: Mapping[str, object], local_pack_root: Path) -> str:
    incoming = str(handoff["incoming_root"])
    evidence_name = f"superseded-runtime-compatibility-evidence-{handoff['run_slug']}"
    return f"""# Lieu: Windows PowerShell local. Durée attendue: 1-4 min; maximum: 15 min.
# Prompts: SSH host-key/password possibles. Ctrl+C arrête seulement la récupération.
# Monitoring: la campagne et systemd ne sont jamais modifiés.
# Signal: PREDICTION_WINDOWS_SUPERSEDED_COMPATIBILITY_RETRIEVED_AUTHENTICATED.
$ErrorActionPreference = 'Stop'
$PackRoot = (Resolve-Path -LiteralPath '{local_pack_root}').Path
$EvidenceRoot = Join-Path (Split-Path -Parent $PackRoot) '{evidence_name}'
if (Test-Path -LiteralPath $EvidenceRoot) {{ throw 'Compatibility evidence root must be new.' }}
$null = New-Item -ItemType Directory -Path $EvidenceRoot
$Target = $env:HYPERLAB_PM_SSH_TARGET
$Key = (Resolve-Path -LiteralPath $env:HYPERLAB_PM_SSH_KEY).Path
$Incoming = '{incoming}'
$Files = @('{RUNTIME_REPORT_NAME}','{REPORT_NAME}','{REPORT_NAME}.sha256','{OUTPUT_INVENTORY_NAME}','{OUTPUT_INVENTORY_NAME}.sha256')
foreach ($Name in $Files) {{
    scp -i $Key "${{Target}}:${{Incoming}}/$Name" (Join-Path $EvidenceRoot $Name)
    if ($LASTEXITCODE -ne 0) {{ throw "Compatibility evidence retrieval failed: $Name" }}
}}
$Python = (Get-Command python).Source
& $Python -I (Join-Path $PackRoot 'scripts/superseded_compat_proof.py') verify-output --proof-root $PackRoot --evidence-root $EvidenceRoot
if ($LASTEXITCODE -ne 0) {{ throw 'Compatibility evidence authentication failed.' }}
Write-Output 'PREDICTION_WINDOWS_SUPERSEDED_COMPATIBILITY_RETRIEVED_AUTHENTICATED'
"""


def _validate_shell(payload: bytes, *, label: str) -> None:
    if (
        not payload.startswith(b"#!/usr/bin/env bash\n")
        or not payload.endswith(b"\n")
        or b"\r" in payload
        or b"\0" in payload
        or payload.startswith(b"\xef\xbb\xbf")
    ):
        raise CompatibilityProofError(f"unsafe Bash payload: {label}")
    payload.decode("utf-8", errors="strict")


def _transfer_inventory(root: Path) -> dict[str, object]:
    return {
        "files": [
            {
                "path": relative,
                "sha256": sha256_file(root / relative),
                "size": (root / relative).stat().st_size,
            }
            for relative in sorted(_TRANSFER_PATHS)
        ],
        "schema_version": 1,
    }


def finalize(
    *,
    repo_root: Path,
    output_root: Path,
    bundle_path: Path,
    source_commit: str,
    run_slug: str,
) -> dict[str, object]:
    if _COMMIT.fullmatch(source_commit) is None or _RUN_SLUG.fullmatch(run_slug) is None:
        raise CompatibilityProofError("candidate commit or proof slug is invalid")
    if _git(repo_root, "rev-parse", "HEAD") != source_commit:
        raise CompatibilityProofError("candidate HEAD diverged")
    if _git(repo_root, "branch", "--show-current") != EXPECTED_BRANCH:
        raise CompatibilityProofError("candidate branch diverged")
    if _git(repo_root, "status", "--porcelain"):
        raise CompatibilityProofError("candidate worktree must be clean")
    if not output_root.is_dir() or output_root.resolve(strict=True) != output_root:
        raise CompatibilityProofError("proof output root is unsafe")
    if bundle_path != output_root / BUNDLE_NAME or not bundle_path.is_file():
        raise CompatibilityProofError("proof bundle path diverged")
    source_inventory = _source_inventory(repo_root, source_commit)
    _write_new(
        output_root / "source-inventory.json",
        canonical_json_bytes(source_inventory) + b"\n",
    )
    for name in _SOURCE_FILES:
        relative = f"ops/prediction_markets_launch_v1/{name}"
        _write_new(output_root / "scripts" / name, _git_bytes(repo_root, source_commit, relative))

    base = "/mnt/HC_Volume_106716684/hyperlab-prediction-markets"
    incoming = f"/home/hyperlab/hyperlab-prediction-markets/incoming/{run_slug}"
    handoff_base: dict[str, object] = {
        "boundary": BOUNDARY,
        "bundle_filename": BUNDLE_NAME,
        "bundle_sha256": sha256_file(bundle_path),
        "campaign_root": f"{base}/campaigns/{run_slug}",
        "dashboard_port": 18081,
        "disk": {
            "h1_reserved_bytes": 154_618_822_656,
            "prediction_maximum_raw_bytes": 22_548_578_304,
            "required_free_bytes": 194_347_270_144,
            "safety_margin_bytes": 17_179_869_184,
        },
        "incoming_root": incoming,
        "proof_kind": PROOF_KIND,
        "run_slug": run_slug,
        "schema_version": 1,
        "service_user": "hyperlab",
        "services": _service_names(run_slug),
        "source_commit": source_commit,
        "source_inventory_sha256": source_inventory["inventory_sha256"],
        "source_root": f"{base}/sources/{run_slug}",
        "superseded_campaign": _target_contract(),
        "volume_base": base,
        "volume_mount": "/mnt/HC_Volume_106716684",
    }
    _write_new(output_root / "README.md", _render_readme(handoff_base).encode("utf-8"))
    operators = {
        "A1-windows-transfer-authenticated.ps1": _render_a1(handoff_base),
        "B1-tabby-superseded-readonly-proof.sh": _render_b1(handoff_base),
        "C1-windows-retrieve-authenticate.ps1": _render_c1(handoff_base, output_root),
    }
    for name, text in operators.items():
        payload = text.encode("utf-8")
        if name.endswith(".sh"):
            _validate_shell(payload, label=name)
        _write_new(output_root / "operator" / name, payload, executable=name.endswith(".sh"))
    transfer = _transfer_inventory(output_root)
    transfer_payload = canonical_json_bytes(transfer) + b"\n"
    _write_new(output_root / "transfer-inventory.json", transfer_payload)
    handoff = {
        **handoff_base,
        "transfer_inventory_sha256": sha256_bytes(transfer_payload),
    }
    handoff_payload = canonical_json_bytes(handoff) + b"\n"
    _write_new(output_root / "handoff.json", handoff_payload)
    _write_new(
        output_root / "handoff.sha256",
        f"{sha256_bytes(handoff_payload)}  handoff.json\n".encode("ascii"),
    )
    return handoff


def _load_handoff(root: Path) -> tuple[dict[str, Any], bytes]:
    handoff, raw = _canonical_object(root / "handoff.json")
    pin = _safe_bytes(root / "handoff.sha256", maximum=256).decode("ascii").split()
    if len(pin) != 2 or pin[1] != "handoff.json" or pin[0] != sha256_bytes(raw):
        raise CompatibilityProofError("proof handoff pin diverged")
    if handoff.get("boundary") != BOUNDARY or handoff.get("proof_kind") != PROOF_KIND:
        raise CompatibilityProofError("proof handoff boundary or kind diverged")
    return handoff, raw


def _verify_bundle(root: Path, handoff: Mapping[str, object]) -> None:
    bundle = root / BUNDLE_NAME
    if sha256_file(bundle) != handoff.get("bundle_sha256"):
        raise CompatibilityProofError("proof bundle SHA-256 diverged")
    temporary = root / f".compatibility-bundle-verify-{uuid4().hex}"
    if temporary.exists():
        raise CompatibilityProofError("bundle verification root is not new")
    try:
        subprocess.run(
            ["git", "init", "--bare", "--quiet", str(temporary)],
            check=True,
            capture_output=True,
            timeout=30,
        )
        verified = subprocess.run(
            ["git", "-C", str(temporary), "bundle", "verify", str(bundle)],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if verified.returncode != 0:
            raise CompatibilityProofError(verified.stderr.strip() or "bundle verify failed")
        heads = subprocess.run(
            ["git", "bundle", "list-heads", str(bundle)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        expected = f"{handoff['source_commit']} refs/heads/{EXPECTED_BRANCH}"
        if heads.returncode != 0 or heads.stdout.strip() != expected:
            raise CompatibilityProofError("proof bundle ref or commit diverged")
    finally:
        if temporary.exists():
            resolved = temporary.resolve(strict=True)
            if resolved.parent != root or not resolved.name.startswith(
                ".compatibility-bundle-verify-"
            ):
                raise CompatibilityProofError("unsafe bundle verification cleanup")
            shutil.rmtree(resolved)


def verify_input(root: Path, *, require_remote_layout: bool) -> dict[str, object]:
    root = root.resolve(strict=True)
    handoff, _ = _load_handoff(root)
    if require_remote_layout and root.as_posix() != handoff.get("incoming_root"):
        raise CompatibilityProofError("remote proof root path diverged")
    inventory, inventory_raw = _canonical_object(root / "transfer-inventory.json")
    if sha256_bytes(inventory_raw) != handoff.get("transfer_inventory_sha256"):
        raise CompatibilityProofError("proof transfer inventory SHA-256 diverged")
    rows = inventory.get("files")
    if not isinstance(rows, list) or [row.get("path") for row in rows if isinstance(row, dict)] != sorted(_TRANSFER_PATHS):
        raise CompatibilityProofError("proof transfer inventory path set diverged")
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"path", "sha256", "size"}:
            raise CompatibilityProofError("proof transfer inventory row is malformed")
        relative = PurePosixPath(str(row["path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise CompatibilityProofError("proof transfer path is unsafe")
        path = root.joinpath(*relative.parts)
        raw = _safe_bytes(path)
        if len(raw) != row["size"] or sha256_bytes(raw) != row["sha256"]:
            raise CompatibilityProofError(f"proof transfer file diverged: {relative}")
        if relative.as_posix().endswith(".sh"):
            _validate_shell(raw, label=relative.as_posix())
    source_inventory, _ = _canonical_object(root / "source-inventory.json")
    if (
        source_inventory.get("commit") != handoff.get("source_commit")
        or source_inventory.get("inventory_sha256")
        != handoff.get("source_inventory_sha256")
    ):
        raise CompatibilityProofError("candidate source inventory binding diverged")
    _verify_bundle(root, handoff)
    return {
        "bundle_sha256": handoff["bundle_sha256"],
        "files": len(rows),
        "run_slug": handoff["run_slug"],
        "source_commit": handoff["source_commit"],
        "terminal_signal": "PREDICTION_SUPERSEDED_COMPATIBILITY_INPUT_GREEN",
    }


def verify_source(root: Path, source_root: Path) -> dict[str, object]:
    handoff, _ = _load_handoff(root.resolve(strict=True))
    source_root = source_root.resolve(strict=True)
    if source_root.as_posix() != handoff.get("source_root"):
        raise CompatibilityProofError("candidate proof source root diverged")
    if _git(source_root, "rev-parse", "HEAD") != handoff.get("source_commit"):
        raise CompatibilityProofError("candidate proof source commit diverged")
    if _git(source_root, "status", "--porcelain", "--untracked-files=all"):
        raise CompatibilityProofError("candidate proof source is not clean")
    expected, _ = _canonical_object(root / "source-inventory.json")
    actual = _source_inventory(source_root, str(handoff["source_commit"]))
    if expected != actual:
        raise CompatibilityProofError("candidate proof source inventory diverged")
    files = actual.get("files")
    if not isinstance(files, list):
        raise CompatibilityProofError("candidate proof source file list is malformed")
    return {
        "files": len(files),
        "inventory_sha256": actual["inventory_sha256"],
        "terminal_signal": "PREDICTION_SUPERSEDED_COMPATIBILITY_SOURCE_GREEN",
    }


def finalize_output(root: Path, cutover_output: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    handoff, _ = _load_handoff(root)
    expected_output = root / "superseded-runtime-compatibility-verify-old.stdout"
    if cutover_output.resolve(strict=True) != expected_output:
        raise CompatibilityProofError("verify-old output path diverged")
    raw = _safe_bytes(cutover_output, maximum=32 * 1024 * 1024)
    if b"\r" in raw or not raw.endswith(b"\n"):
        raise CompatibilityProofError("verify-old output framing diverged")
    lines = raw.decode("utf-8").splitlines()
    expected_signals = [
        "PREDICTION_OLD_RAW_RECEIPTS_LEDGER_AUTHENTICATED",
        "PREDICTION_OLD_CAMPAIGN_FIVE_UNITS_AUTHENTICATED",
        "PREDICTION_OLD_CAMPAIGN_PREMUTATION_AUTHENTICATED",
    ]
    if len(lines) != 4 or lines[1:] != expected_signals:
        raise CompatibilityProofError("verify-old terminal signal sequence diverged")
    try:
        runtime = json.loads(lines[0])
    except json.JSONDecodeError as error:
        raise CompatibilityProofError("compatibility runtime report is invalid") from error
    if not isinstance(runtime, dict):
        raise CompatibilityProofError("compatibility runtime report is not an object")
    claimed = runtime.get("compatibility_sha256")
    runtime_body = {key: value for key, value in runtime.items() if key != "compatibility_sha256"}
    candidate_tool = runtime.get("candidate_tool")
    modules = runtime.get("modules")
    alias = modules.get("__mp_main__") if isinstance(modules, dict) else None
    if (
        not isinstance(candidate_tool, dict)
        or not isinstance(alias, dict)
        or alias.get("alias_of") != "__main__"
        or alias.get("class") != "candidate_tool"
        or {key: value for key, value in alias.items() if key != "alias_of"}
        != candidate_tool
        or type(runtime.get("loaded_module_files_validated")) is not int
        or runtime["loaded_module_files_validated"] < 1
    ):
        raise CompatibilityProofError("candidate tool alias report binding diverged")
    if (
        claimed != sha256_bytes(canonical_json_bytes(runtime_body))
        or runtime.get("adapter_id") != ADAPTER_ID
        or runtime.get("candidate_commit") != handoff.get("source_commit")
        or runtime.get("target_commit") != TARGET_COMMIT
        or runtime.get("target_inventory_sha256") != TARGET_INVENTORY_SHA256
        or runtime.get("no_historical_new_cli_invoked") is not True
        or runtime.get("terminal_signal")
        != "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_RUNTIME_GREEN"
    ):
        raise CompatibilityProofError("compatibility runtime report binding diverged")
    runtime_payload = canonical_json_bytes(runtime) + b"\n"
    _write_new(root / RUNTIME_REPORT_NAME, runtime_payload)
    body: dict[str, object] = {
        "adapter_id": ADAPTER_ID,
        "boundary": BOUNDARY,
        "candidate_commit": handoff["source_commit"],
        "candidate_inventory_sha256": handoff["source_inventory_sha256"],
        "cutover_output_sha256": sha256_bytes(raw),
        "no_cutover": True,
        "no_historical_new_cli_invoked": True,
        "proof_kind": PROOF_KIND,
        "run_slug": handoff["run_slug"],
        "runtime_compatibility_sha256": claimed,
        "schema_version": 1,
        "system_and_evidence_signals": expected_signals,
        "target_commit": TARGET_COMMIT,
        "target_inventory_sha256": TARGET_INVENTORY_SHA256,
        "target_run_slug": TARGET_SLUG,
        "terminal_signal": (
            "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER"
        ),
    }
    report = {**body, "proof_sha256": sha256_bytes(canonical_json_bytes(body))}
    report_payload = canonical_json_bytes(report) + b"\n"
    _write_new(root / REPORT_NAME, report_payload)
    _write_new(
        root / f"{REPORT_NAME}.sha256",
        f"{sha256_bytes(report_payload)}  {REPORT_NAME}\n".encode("ascii"),
    )
    output_inventory = {
        "files": [
            {
                "path": name,
                "sha256": sha256_file(root / name),
                "size": (root / name).stat().st_size,
            }
            for name in _OUTPUT_FILES
        ],
        "schema_version": 1,
    }
    inventory_payload = canonical_json_bytes(output_inventory) + b"\n"
    _write_new(root / OUTPUT_INVENTORY_NAME, inventory_payload)
    _write_new(
        root / f"{OUTPUT_INVENTORY_NAME}.sha256",
        f"{sha256_bytes(inventory_payload)}  {OUTPUT_INVENTORY_NAME}\n".encode("ascii"),
    )
    return report


def verify_output(proof_root: Path, evidence_root: Path) -> dict[str, object]:
    handoff, _ = _load_handoff(proof_root.resolve(strict=True))
    evidence_root = evidence_root.resolve(strict=True)
    inventory, inventory_raw = _canonical_object(evidence_root / OUTPUT_INVENTORY_NAME)
    pin = _safe_bytes(
        evidence_root / f"{OUTPUT_INVENTORY_NAME}.sha256", maximum=256
    ).decode("ascii").split()
    if len(pin) != 2 or pin != [sha256_bytes(inventory_raw), OUTPUT_INVENTORY_NAME]:
        raise CompatibilityProofError("output inventory pin diverged")
    rows = inventory.get("files")
    if not isinstance(rows, list) or [row.get("path") for row in rows if isinstance(row, dict)] != list(_OUTPUT_FILES):
        raise CompatibilityProofError("output inventory file set diverged")
    for row in rows:
        if not isinstance(row, dict):
            raise CompatibilityProofError("output inventory row is malformed")
        path = evidence_root / str(row["path"])
        raw = _safe_bytes(path)
        if len(raw) != row.get("size") or sha256_bytes(raw) != row.get("sha256"):
            raise CompatibilityProofError(f"output evidence diverged: {path.name}")
    report, report_raw = _canonical_object(evidence_root / REPORT_NAME)
    report_pin = _safe_bytes(
        evidence_root / f"{REPORT_NAME}.sha256", maximum=256
    ).decode("ascii").split()
    body = {key: value for key, value in report.items() if key != "proof_sha256"}
    if (
        report_pin != [sha256_bytes(report_raw), REPORT_NAME]
        or report.get("proof_sha256") != sha256_bytes(canonical_json_bytes(body))
        or report.get("candidate_commit") != handoff.get("source_commit")
        or report.get("target_commit") != TARGET_COMMIT
        or report.get("terminal_signal")
        != "PREDICTION_SUPERSEDED_RUNTIME_COMPATIBILITY_GREEN_NO_CUTOVER"
    ):
        raise CompatibilityProofError("compatibility proof report binding diverged")
    return {
        "candidate_commit": report["candidate_commit"],
        "output_inventory_sha256": sha256_bytes(inventory_raw),
        "proof_sha256": report["proof_sha256"],
        "run_slug": report["run_slug"],
        "terminal_signal": (
            "PREDICTION_WINDOWS_SUPERSEDED_COMPATIBILITY_RETRIEVED_AUTHENTICATED"
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Superseded runtime compatibility proof pack")
    sub = parser.add_subparsers(dest="command", required=True)
    final = sub.add_parser("finalize")
    final.add_argument("--repo-root", type=Path, required=True)
    final.add_argument("--output-root", type=Path, required=True)
    final.add_argument("--bundle", type=Path, required=True)
    final.add_argument("--source-commit", required=True)
    final.add_argument("--run-slug", required=True)
    verify = sub.add_parser("verify-input")
    verify.add_argument("--proof-root", type=Path, required=True)
    verify.add_argument("--require-remote-layout", action="store_true")
    source = sub.add_parser("verify-source")
    source.add_argument("--proof-root", type=Path, required=True)
    source.add_argument("--source-root", type=Path, required=True)
    output = sub.add_parser("finalize-output")
    output.add_argument("--proof-root", type=Path, required=True)
    output.add_argument("--cutover-output", type=Path, required=True)
    retrieve = sub.add_parser("verify-output")
    retrieve.add_argument("--proof-root", type=Path, required=True)
    retrieve.add_argument("--evidence-root", type=Path, required=True)
    return parser


def _fail(message: str) -> NoReturn:
    print(f"PREDICTION_SUPERSEDED_COMPATIBILITY_PROOF_REFUSED:{message}", file=sys.stderr)
    raise SystemExit(4)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "finalize":
            result = finalize(
                repo_root=args.repo_root.resolve(strict=True),
                output_root=args.output_root.resolve(strict=True),
                bundle_path=args.bundle.resolve(strict=True),
                source_commit=args.source_commit,
                run_slug=args.run_slug,
            )
        elif args.command == "verify-input":
            result = verify_input(
                args.proof_root,
                require_remote_layout=args.require_remote_layout,
            )
        elif args.command == "verify-source":
            result = verify_source(args.proof_root, args.source_root)
        elif args.command == "finalize-output":
            result = finalize_output(args.proof_root, args.cutover_output)
        else:
            result = verify_output(args.proof_root, args.evidence_root)
        print(canonical_json_bytes(result).decode("utf-8"))
        return 0
    except (CompatibilityProofError, OSError, subprocess.SubprocessError) as error:
        _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
