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
$ExpectedBranch = 'codex/h1-v7-dashboard-binding-v1'
$ExpectedRef = "refs/heads/$ExpectedBranch"
$Python = 'C:\Dev\hyperlab-multistrategy\.venv\Scripts\python.exe'
$InputPath = Join-Path $PSScriptRoot 'binding-input-v8-v2.json'

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Canonical local Python is absent: $Python"
}
if ((git -C $RepoRoot rev-parse HEAD) -ne $Commit) {
    throw 'Local HEAD differs from the requested final commit.'
}
if ((git -C $RepoRoot branch --show-current) -ne $ExpectedBranch) {
    throw 'Current branch differs from the frozen historical binding branch.'
}
if ((git -C $RepoRoot rev-parse $ExpectedRef) -ne $Commit) {
    throw 'Target branch ref differs from the requested final commit.'
}
if (git -C $RepoRoot status --porcelain) {
    throw 'Dashboard binding worktree must be clean before bundle creation.'
}
if (Test-Path -LiteralPath $OutputRoot) {
    throw "Output root must be new: $OutputRoot"
}

$env:PYTHONPATH = Join-Path $RepoRoot 'src'
& $Python -B (Join-Path $PSScriptRoot 'binding_pack.py') inspect-input --input $InputPath
if ($LASTEXITCODE -ne 0) { throw 'Frozen V8 dashboard binding V2 input refused.' }

New-Item -ItemType Directory -Path $OutputRoot | Out-Null
$BundlePath = Join-Path $OutputRoot 'hyperlab-h1-v8-dashboard-binding-v2.bundle'
git -C $RepoRoot bundle create $BundlePath $ExpectedRef
if ($LASTEXITCODE -ne 0) { throw 'git bundle create failed.' }
git -C $RepoRoot bundle verify $BundlePath
if ($LASTEXITCODE -ne 0) { throw 'git bundle verify failed.' }

& $Python -B (Join-Path $PSScriptRoot 'binding_pack.py') finalize `
    --repo-root $RepoRoot `
    --input $InputPath `
    --bundle $BundlePath `
    --output-root $OutputRoot `
    --source-commit $Commit
if ($LASTEXITCODE -ne 0) { throw 'H1 V8 dashboard binding V2 finalization failed.' }

Get-FileHash -LiteralPath $BundlePath -Algorithm SHA256
Get-FileHash -LiteralPath (Join-Path $OutputRoot 'handoff.json') -Algorithm SHA256
Get-FileHash -LiteralPath (Join-Path $OutputRoot 'binding-files.sha256') -Algorithm SHA256
Get-Content -LiteralPath (Join-Path $OutputRoot 'binding-files.sha256')
Write-Output 'H1_V8_DASHBOARD_BINDING_V2_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED'
