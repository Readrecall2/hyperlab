# Workflow Codex

## Répartition des rôles

- **ChatGPT dans la conversation** : architecture, hypothèses, analyse des rapports et décisions de phase ;
- **Codex local** : modifications du dépôt, tests, import de données et rapports ;
- **toi** : Git, autorisations sensibles, wallets et passage de phase ;
- **bot** : règles déterministes seulement, sans appel à un LLM en production.

## Configuration recommandée

Ouvrez le dossier dans Codex. Utilisez le sandbox limité au workspace et les approbations à la demande. L'accès réseau n'est accordé que pour les dépendances ou les APIs publiques nécessaires. Ne donnez jamais à Codex un accès au wallet principal.

## Première session

```text
Lis entièrement AGENTS.md, README.md, docs/GUIDE_COMPLET_FR.md et le dossier docs/.
Ne modifie rien.

1. Cartographie l'architecture.
2. Prouve qu'aucun chemin ne peut envoyer un ordre.
3. Exécute ruff, mypy et pytest.
4. Exécute la démo synthétique.
5. Signale les erreurs confirmées et les hypothèses fragiles.
6. Propose un plan de correction minimal, sans l'appliquer.
```

Après l'audit :

```text
Corrige uniquement les défauts confirmés.
Respecte AGENTS.md.
Ajoute les tests de régression.
Exécute ruff, mypy, pytest et la démo.
Fais une revue critique de ton diff.
Ne crée aucune authentification, clé ni exécuteur.
```

## Une branche par phase

```powershell
git switch main
git pull
git switch -c phase-02-hl-data
```

Après Codex :

```powershell
git status
git diff
.\.venv\Scripts\python.exe -m pytest
git add .
git commit -m "feat: add validated Hyperliquid data ingestion"
```

## Ordre des prompts

Exécutez les fichiers `prompts/00_...` à `prompts/15_...` dans l'ordre. Ne fusionnez pas plusieurs phases dans un seul prompt. Chaque prompt contient une définition de terminé.

## Discipline contre le surapprentissage

Codex doit journaliser :

- chaque variante ;
- chaque paramètre ;
- la période regardée ;
- la métrique utilisée pour sélectionner ;
- les résultats négatifs ;
- le nombre total d'essais.

Un rapport spectaculaire doit déclencher un audit plus strict, pas un passage accéléré au réel.
