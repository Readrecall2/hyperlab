$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "=== HyperLab : installation Windows 11 ===" -ForegroundColor Cyan

$Launcher = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    & py -3.12 -c "import sys; print(sys.version)" *> $null
    if ($LASTEXITCODE -eq 0) {
        $Launcher = @("py", "-3.12")
    } else {
        & py -3.11 -c "import sys; print(sys.version)" *> $null
        if ($LASTEXITCODE -eq 0) {
            $Launcher = @("py", "-3.11")
        }
    }
}
if (-not $Launcher -and (Get-Command python -ErrorAction SilentlyContinue)) {
    & python -c "import sys; assert (3, 11) <= sys.version_info[:2] < (3, 14)" *> $null
    if ($LASTEXITCODE -eq 0) {
        $Launcher = @("python")
    }
}
if (-not $Launcher) {
    throw "Python 3.11 ou 3.12 est requis. Installez-le avec: winget install -e --id Python.Python.3.12"
}

if ($Launcher.Count -eq 2) {
    & $Launcher[0] $Launcher[1] -m venv .venv
} else {
    & $Launcher[0] -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --require-hashes --requirement requirements-ci.lock
& .\.venv\Scripts\python.exe -m pip install --no-deps --editable .
& .\.venv\Scripts\python.exe -m pytest
& .\.venv\Scripts\python.exe -m hyperlab doctor

Write-Host "`nInstallation terminée." -ForegroundColor Green
Write-Host "Démo: .\.venv\Scripts\python.exe -m hyperlab demo"
