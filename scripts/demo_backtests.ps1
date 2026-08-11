$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
& .\.venv\Scripts\python.exe -m hyperlab demo --strategy all --hours 1200 --output reports\demo
Write-Host "Ouvrez reports\demo\comparison.html" -ForegroundColor Green
