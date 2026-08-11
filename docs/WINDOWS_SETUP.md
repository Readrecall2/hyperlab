# Installation sur Windows 11

## 1. Prérequis

Ouvrez PowerShell normalement, sans administrateur :

```powershell
winget install -e --id Python.Python.3.12
winget install -e --id Git.Git
winget install -e --id Docker.DockerDesktop
```

Redémarrez le terminal après l'installation.

## 2. Extraire le projet

Exemple :

```powershell
New-Item -ItemType Directory -Force C:\Dev
Expand-Archive "$HOME\Downloads\hyperlab-multistrategy-v0.2.0.zip" C:\Dev -Force
cd C:\Dev\hyperlab-multistrategy
```

Le nom exact du dossier extrait peut varier ; entrez dans celui qui contient `pyproject.toml`.

## 3. Bootstrap

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\bootstrap.ps1
```

Le script crée `.venv`, installe les dépendances, exécute les tests puis `hyperlab doctor`.

## 4. Démonstration de toutes les stratégies

```powershell
.\.venv\Scripts\python.exe -m hyperlab strategies
.\.venv\Scripts\python.exe -m hyperlab demo --strategy all
Start-Process .\reports\demo\comparison.html
```

Les données sont synthétiques. Le rapport valide seulement le moteur et l'affichage.

## 5. Premier snapshot public

```powershell
.\.venv\Scripts\python.exe -m hyperlab snapshot --network mainnet --save
.\.venv\Scripts\python.exe -m hyperlab status
```

Aucun wallet n'est requis.

## 6. Collecte de dix minutes

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect `
  --interval-seconds 60 `
  --samples 10 `
  --network mainnet
```

## 7. Dashboard local

Terminal 1 :

```powershell
.\.venv\Scripts\python.exe -m hyperlab serve --host 127.0.0.1 --port 8000
```

Puis ouvrez `http://127.0.0.1:8000`.

## 8. Git avant Codex

```powershell
git init
git add .
git commit -m "chore: initial HyperLab multi-strategy research lab"
git switch -c phase-01-data
```

Commandes de contrôle :

```powershell
git status
git diff
git log --oneline
```

## 9. Docker local

```powershell
.\scripts\build_docker.ps1
docker compose up -d
docker compose ps
docker compose logs -f collector
```

Le dashboard Docker écoute uniquement sur `127.0.0.1:8000`.

Arrêt :

```powershell
docker compose down
```
