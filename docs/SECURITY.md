# Sécurité

## Version actuelle

HyperLab 0.2.0 n'accepte aucun secret et n'importe pas l'API d'échange. Une recherche automatique peut le vérifier :

```powershell
Get-ChildItem -Recurse -File src | Select-String "hyperliquid.exchange|private_key|seed_phrase"
```

## Hygiène Git

Avant chaque push :

```powershell
git diff --cached
git grep -n -I -E "(private.?key|seed.?phrase|mnemonic|0x[a-fA-F0-9]{64})"
```

Utilisez aussi la protection de secrets du fournisseur Git.

## Futur exécuteur

Il sera dans un service séparé avec :

- API wallet dédiée ;
- adresse principale publique séparée du signer ;
- keystore chiffré hors dépôt ;
- aucun accès depuis le dashboard ;
- liste blanche de stratégies et marchés ;
- limites codées et configuration signée ;
- journal append-only ;
- dead-man switch ;
- bouton de révocation documenté.

## Menaces à tester

- réponse réseau perdue après un ordre accepté ;
- événement WebSocket dupliqué ou manquant ;
- clock drift ;
- données obsolètes ;
- redémarrage avec positions ouvertes ;
- corruption locale ;
- injection via dashboard ;
- dépendance compromise ;
- conteneur escaladant vers l'hôte ;
- erreur humaine de réseau testnet/mainnet.
