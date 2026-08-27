[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Commit,

    [Parameter(Mandatory = $true)]
    [string] $OutputRoot,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^pm-[0-9]{8}t[0-9]{6}z-[0-9a-f]{8}$')]
    [string] $RunSlug,

    [string] $Python = 'C:\Dev\hyperlab-multistrategy\.venv\Scripts\python.exe'
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$ExpectedBranch = 'codex/prediction-markets-prospective-launch-v1'
$ExpectedRef = "refs/heads/$ExpectedBranch"

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Canonical local Python is absent: $Python"
}
if ((git -C $RepoRoot rev-parse HEAD) -ne $Commit) {
    throw 'Local HEAD differs from the requested final commit.'
}
if ((git -C $RepoRoot rev-parse $ExpectedRef) -ne $Commit) {
    throw 'Target branch differs from the requested final commit.'
}
if (git -C $RepoRoot status --porcelain) {
    throw 'The launch worktree must be clean before bundle creation.'
}
if (Test-Path -LiteralPath $OutputRoot) {
    throw "Output root must be new: $OutputRoot"
}

Write-Output 'Lieu: Windows PowerShell local.'
Write-Output 'Durée attendue: 3-12 min; maximum: 35 min.'
Write-Output 'Prompts: aucun pour Git/Python; téléchargement strictement limité au lock hashé.'
Write-Output 'Ctrl+C laisse une sortie incomplète non transférable; recommencer avec un nouvel OutputRoot/RunSlug.'
New-Item -ItemType Directory -Path $OutputRoot | Out-Null
$BundlePath = Join-Path $OutputRoot 'hyperlab-prediction-markets-prospective-launch-v1.bundle'
git -C $RepoRoot bundle create $BundlePath $ExpectedRef
if ($LASTEXITCODE -ne 0) { throw 'git bundle create failed.' }
git bundle verify $BundlePath
if ($LASTEXITCODE -ne 0) { throw 'git bundle verify failed.' }

$Wheelhouse = Join-Path $OutputRoot 'wheelhouse'
New-Item -ItemType Directory -Path $Wheelhouse | Out-Null
$env:PIP_DISABLE_PIP_VERSION_CHECK = '1'
$env:PIP_NO_INPUT = '1'
& $Python -m pip download `
    --dest $Wheelhouse `
    --require-hashes `
    --only-binary=:all: `
    --no-deps `
    --platform manylinux_2_28_x86_64 `
    --platform manylinux_2_17_x86_64 `
    --implementation cp `
    --python-version 312 `
    --abi cp312 `
    --requirement (Join-Path $RepoRoot 'requirements-runtime.lock')
if ($LASTEXITCODE -ne 0) { throw 'Hash-locked Linux wheelhouse acquisition failed.' }

& $Python (Join-Path $PSScriptRoot 'launch_pack.py') finalize `
    --repo-root $RepoRoot `
    --plan (Join-Path $PSScriptRoot 'launch-plan-v1.json') `
    --output-root $OutputRoot `
    --bundle $BundlePath `
    --source-commit $Commit `
    --run-slug $RunSlug
if ($LASTEXITCODE -ne 0) { throw 'Prediction Markets launch-pack finalization failed.' }
Get-FileHash -LiteralPath $BundlePath -Algorithm SHA256
Get-FileHash -LiteralPath (Join-Path $OutputRoot 'handoff.json') -Algorithm SHA256
Get-Content -LiteralPath (Join-Path $OutputRoot 'wheelhouse.sha256')
Write-Output 'PREDICTION_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED'
