# Store Umbrel privé HyperLab

Les fichiers `umbrel-app-store.yml` et `jjlab-hyperlab/` restent à la racine du dépôt GitHub, conformément au template officiel des Community App Stores Umbrel.

Avant publication, remplacez les placeholders avec :

```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB --repository hyperlab --tag 0.2.0
```

L'application contient uniquement :

- un collecteur de données publiques ;
- un dashboard local en lecture seule ;
- aucun secret ;
- aucun exécuteur d'ordres.
