# Installation sur umbrelOS

## Rôle retenu

Au départ, Umbrel fait seulement tourner :

```text
collector public 24/24 + SQLite + dashboard read-only
```

Il ne reçoit aucune clé et ne place aucun ordre. Les backtests lourds, Codex et Git restent sur Windows 11.

## Prérequis

- un compte GitHub ;
- Docker Desktop sur Windows ;
- le projet déjà testé avec `bootstrap.ps1` ;
- un dépôt GitHub pour le code et le Community App Store ;
- GitHub Container Registry pour l'image `amd64`/`arm64`.

## 1. Créer le dépôt GitHub

Créez un dépôt nommé `hyperlab`, puis :

```powershell
git remote add origin https://github.com/VOTRE_NOM_GITHUB/hyperlab.git
git branch -M main
git push -u origin main
```

Utilise un dépôt public pour cette version : il ne contient aucun secret, et Umbrel doit pouvoir lire le store ainsi que télécharger l'image GHCR sans authentification.

## 2. Personnaliser le store

Les fichiers `umbrel-app-store.yml` et `jjlab-hyperlab/` sont volontairement à la **racine du dépôt** : c'est la structure attendue par le template officiel Umbrel.


```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB

git add umbrel-app-store.yml jjlab-hyperlab scripts\prepare_umbrel_store.py
git commit -m "chore: configure private Umbrel app store"
git push
```

Vérifiez qu'il ne reste aucun placeholder :

```powershell
Get-ChildItem .\jjlab-hyperlab -Recurse -File | Select-String "REPLACE_WITH_"
```

La commande ne doit rien retourner.

## 3. Publier l'image Docker

Le workflow `.github/workflows/container.yml` publie l'image lors d'un tag.

```powershell
git tag v0.2.0
git push origin v0.2.0
```

Dans GitHub :

1. ouvrez l'onglet **Actions** et vérifiez que `Publish container` réussit ;
2. ouvrez **Packages**, puis le package `hyperlab` ;
3. ouvrez **Package settings → Change visibility → Public** ;
4. vérifiez que le tag `0.2.0` existe pour `linux/amd64` et `linux/arm64`.

Umbrel ne se connecte pas automatiquement à un registre GHCR privé. Pour cette version sans secret, une image publique est la solution la plus simple et la plus auditable.

Ne mettez jamais de clé dans les variables GitHub Actions de cette version.

## 4. Ajouter le Community App Store dans Umbrel

Dans l'interface umbrelOS :

1. ouvrez **App Store** ;
2. cliquez sur les trois points en haut à droite ;
3. choisissez **Community App Stores** ;
4. collez l'URL GitHub du dépôt ;
5. cliquez sur **Add** ;
6. ouvrez le store `Jj HyperLab` ;
7. installez **HyperLab**.

Les Community App Stores ne sont pas vérifiés par Umbrel. N'ajoutez que ton propre dépôt et relisez toujours le `docker-compose.yml` avant installation.

## 5. Vérifier l'installation

Ouvrez l'icône HyperLab. La bannière doit afficher :

```text
READ-ONLY — ORDRES IMPOSSIBLES
```

Après quelques minutes, le compteur de snapshots doit augmenter.

Tests supplémentaires depuis le navigateur :

```text
/health
/api/status
```

Le JSON doit contenir :

```json
{"ok": true, "mode": "readonly", "orders_enabled": false}
```

## 6. Mise à jour

1. modifiez et testez sur Windows ;
2. augmentez la version dans `pyproject.toml` et `umbrel-app.yml` ;
3. créez un nouveau tag ;
4. attendez la publication de l'image ;
5. poussez le manifeste ;
6. mettez à jour l'application dans Umbrel.

N'utilisez jamais `latest` pour une phase critique. Épinglez une version immuable.

## 7. Exporter les données vers Windows

Le chemin exact de données dépend d'umbrelOS et de son UI de fichiers. La méthode la plus simple est d'utiliser l'application Files ou un partage réseau et de copier le dossier persistant HyperLab. Ne modifiez pas directement le système Umbrel avant d'avoir une sauvegarde.

Dans une phase ultérieure, Codex ajoutera une exportation quotidienne Parquet et un bouton de téléchargement depuis le dashboard.

## 8. Limite matérielle

Le mini-PC domestique convient aux stratégies lentes. Pour lead-lag ou market making réellement sub-seconde, mesurez la latence ; un VPS dédié, proche des infrastructures pertinentes, sera probablement plus adapté au live. Umbrel reste néanmoins excellent pour la collecte forward et les expériences non sensibles à quelques dizaines de millisecondes.
