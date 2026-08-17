# Sécurité

## Version actuelle — paper-only

HyperLab 0.2.1 n'accepte aucun secret et n'importe pas l'API privée d'échange. Le
paper engine ne fait que simuler localement le cycle des ordres. Il ne contient ni
wallet, ni signature, ni nonce exchange, ni authentification, ni route permettant
d'envoyer, modifier ou annuler un ordre réel. Même `EMERGENCY_FLATTEN` est une
réduction simulée.

Une recherche automatique peut le vérifier :

```powershell
Get-ChildItem -Recurse -File src | Select-String "hyperliquid.exchange|private_key|seed_phrase|mnemonic"
```

Les frais paper sont chargés depuis un artefact public, versionné et hashé. Il est
interdit d'interroger un endpoint privé de compte pour obtenir un palier ou une
remise. Sans preuve publique, le palier public conservateur est utilisé.

## Hygiène Git

Avant chaque push :

```powershell
git diff --cached
git grep -n -I -E "(private.?key|seed.?phrase|mnemonic|0x[a-fA-F0-9]{64})"
```

Utilisez aussi la protection de secrets du fournisseur Git.

## Cloisonnement du paper engine

- entrées réseau limitées aux flux publics déjà autorisés ;
- aucun SDK ou module de transport privé dans le graphe d'import ;
- SQLite est l'autorité append-only ; les snapshots dashboard sont dérivés ;
- dashboard ouvert en lecture seule, sans endpoint de commande ou de mutation ;
- configuration figée et identifiée par hash ;
- chaque événement porte un ID déterministe et participe à une chaîne de hashes ;
- toute collision d'ID, troncature, corruption ou divergence de replay force
  `MANUAL_REVIEW` ;
- les alertes ne transportent aucun secret et n'ont aucun canal de contrôle retour.

Une stratégie ne reçoit jamais un accès direct aux tables de fills, positions ou
cash. Elle soumet seulement une décision au contrôle de risque et au moteur de
simulation. Cette séparation empêche un module de stratégie de contourner le cycle
d'ordre paper.

## Futures classes d'exécution

Tout reçu est lié à une classe et un but exacts. Une identité absente ou ambiguë
échoue fermée ; il n'existe aucune conversion de reçu ni fallback
Testnet/Mainnet.

L'exécuteur Testnet sera un service séparé avec endpoint/chain ID allowlistés,
credentials Testnet dédiés non réutilisés depuis Mainnet, CLOID déterministes,
réconciliation exchange-first, limites bornées, kill switch et audit complet. Sa
préparation technique ne requiert pas Gates B/C/D.

Un futur exécuteur avec argent réel exige en plus Gates B/C/D/E, revue humaine,
configuration signée et :

- API wallet dédiée ;
- adresse principale publique séparée du signer ;
- keystore chiffré hors dépôt ;
- aucun accès depuis le dashboard ;
- liste blanche de stratégies et marchés ;
- limites codées et configuration signée ;
- journal append-only ;
- dead-man switch ;
- bouton de révocation documenté.

`MICRO_MAINNET` et `MAINNET` consomment des reçus distincts ; Mainnet exige
la preuve Gate F et deux confirmations humaines. HyperLab 0.2.x conserve
`HYPERLAB_MODE=readonly|research` et ne contient aucun de ces exécuteurs.

## Menaces à tester

- crash avant/après commit atomique d'un événement paper ;
- événement live ou paper dupliqué avec contenu identique ou contradictoire ;
- journal modifié ou tronqué et projection obsolète ;
- divergence de replay à configuration/seed identiques ;
- contournement du moteur paper par une stratégie ou le dashboard ;
- apparition d'un import privé, d'une clé, d'un signer ou d'une route d'ordre ;
- réponse réseau perdue après un ordre accepté, pour le futur testnet seulement ;
- événement WebSocket dupliqué ou manquant ;
- clock drift ;
- données obsolètes ;
- redémarrage avec positions ouvertes ;
- corruption locale ;
- injection via dashboard ;
- dépendance compromise ;
- conteneur escaladant vers l'hôte ;
- erreur humaine de réseau testnet/mainnet.
