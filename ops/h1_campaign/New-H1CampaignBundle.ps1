[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Commit,

    [Parameter(Mandatory = $true)]
    [string] $OutputRoot
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$ExpectedBranch = 'codex/h1-prospective-campaign-launch-v1'
$ExpectedRef = "refs/heads/$ExpectedBranch"
$Python = 'C:\Dev\hyperlab-multistrategy\.venv\Scripts\python.exe'

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

New-Item -ItemType Directory -Path $OutputRoot | Out-Null
$BundlePath = Join-Path $OutputRoot 'hyperlab-h1-prospective-campaign-launch-v1.bundle'
git -C $RepoRoot bundle create $BundlePath $ExpectedRef
if ($LASTEXITCODE -ne 0) { throw 'git bundle create failed.' }
git -C $RepoRoot bundle verify $BundlePath
if ($LASTEXITCODE -ne 0) { throw 'git bundle verify failed.' }

$env:PYTHONPATH = Join-Path $RepoRoot 'src'
& $Python (Join-Path $PSScriptRoot 'launch_pack.py') finalize `
    --repo-root $RepoRoot `
    --plan (Join-Path $PSScriptRoot 'launch-plan-v1.json') `
    --bundle $BundlePath `
    --output-root $OutputRoot `
    --source-commit $Commit
if ($LASTEXITCODE -ne 0) { throw 'H1 launch-pack finalization failed.' }

Get-FileHash -LiteralPath $BundlePath -Algorithm SHA256
Get-FileHash -LiteralPath (Join-Path $OutputRoot 'handoff.json') -Algorithm SHA256
Get-Content -LiteralPath (Join-Path $OutputRoot 'launch-files.sha256')
Write-Output 'H1_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED'
