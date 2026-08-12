# Dépannage

## PowerShell bloque le script

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\bootstrap.ps1
```

Le réglage ne vaut que pour le terminal courant.

## `py -3.12` introuvable

```powershell
winget install -e --id Python.Python.3.12
```

Fermez puis rouvrez PowerShell.

## Mauvais dossier

La commande doit être lancée dans le dossier contenant `pyproject.toml` :

```powershell
Get-ChildItem pyproject.toml
```

## Dépendance qui ne s'installe pas

```powershell
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -e ".[dev,research]"
```

Conservez le message complet pour l'analyser ; ne désactivez pas les vérifications TLS.

## `snapshot` échoue

Vérifiez :

```powershell
Resolve-DnsName api.hyperliquid.xyz
Test-NetConnection api.hyperliquid.xyz -Port 443
```

Puis relancez. Un endpoint ou un format peut avoir changé : comparez avec le SDK officiel avant de bricoler le parser.

## Dashboard vide

```powershell
.\.venv\Scripts\python.exe -m hyperlab status
.\.venv\Scripts\python.exe -m hyperlab collect --assets BTC --duration-seconds 120
```

Le dashboard ne fabrique pas de données ; le collecteur doit avoir réussi au moins une fois.

## Docker Desktop ne répond pas

```powershell
docker version
docker compose version
docker compose ps
```

Ouvrez Docker Desktop et attendez que le moteur soit opérationnel, puis reconstruisez.

## L'image GHCR est refusée par Umbrel

- vérifiez le nom exact de l'image dans `docker-compose.yml` ;
- vérifiez que le tag existe ;
- vérifiez qu'il contient l'architecture du mini-PC ;
- vérifiez la visibilité du package ;
- ne placez pas de token GitHub en clair dans le manifeste.

## L'app n'apparaît pas dans le Community App Store

- le dépôt doit contenir `umbrel-app-store.yml` à sa racine ;
- l'ID de l'app doit commencer par l'ID du store : `jjlab-hyperlab` ;
- le dossier doit porter exactement cet ID ;
- les YAML doivent être valides ;
- poussez un petit commit puis actualisez le store.

## L'app démarre mais ne peut pas écrire `/data`

Consultez les logs Umbrel. Vérifiez que le volume `${APP_DATA_DIR}/data:/data` existe. Ne passez pas le conteneur en root par réflexe ; corrigez les permissions du dossier persistant selon la documentation umbrelOS.

## Les tests synthétiques affichent un rendement énorme

C'est possible et volontairement non interprétable. Le générateur contient des motifs connus pour exercer les stratégies. Seuls les résultats sur données réelles, hors échantillon et après coûts ont une valeur de recherche.

## Le market making synthétique perd beaucoup en frais

Ce n'est pas un bug automatique : le modèle peut faire trop de fills ou payer un spread insuffisant. Le simulateur actuel est une maquette. Ne l'optimisez pas comme s'il représentait une vraie queue L2.
