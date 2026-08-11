$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose build
Write-Host "Image locale construite. Lancement: docker compose up -d" -ForegroundColor Green
