# Sécurité

## Paquet racine 0.2.1 — paper-only

HyperLab 0.2.1 n'accepte aucun secret et n'importe pas l'API privée d'échange. Le
paper engine ne fait que simuler localement le cycle des ordres. Il ne contient ni
wallet, ni signature, ni nonce exchange, ni authentification, ni route permettant
d'envoyer, modifier ou annuler un ordre réel. Même `EMERGENCY_FLATTEN` est une
réduction simulée. Ce contrat couvre `src/hyperlab`, le dashboard et Umbrel ; le
service Phase 13 est un paquet local séparé sous `services/testnet-executor`.

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

## Classes d'exécution séparées

Tout reçu est lié à une classe et un but exacts. Une identité absente ou ambiguë
échoue fermée ; il n'existe aucune conversion de reçu ni fallback
Testnet/Mainnet.

L'exécuteur Testnet est le service séparé `services/testnet-executor`, version
`0.3.0.dev0`, identité exacte `TESTNET` / `TESTNET_EXECUTION`. Il accepte
uniquement :

- HTTP `https://api.hyperliquid-testnet.xyz` ;
- WebSocket `wss://api.hyperliquid-testnet.xyz/ws` ;
- chain identity `TESTNET` ;
- namespace de credentials `HYPERLAB_TESTNET`.

Toute différence, valeur implicite, URL Mainnet, redirect ou fallback échoue avant
l'envoi. Le transport signé réutilise uniquement les primitives officielles et
n'importe jamais `hyperliquid.exchange.Exchange`. Le service conserve CLOID
déterministes, nonce/tentative persistés avant I/O, réconciliation exchange-first,
limites bornées, pause/kill, dead-man switch et audit append-only. Sa préparation
technique ne requiert pas Gates B/C/D, mais le checkout ne revendique encore ni
workflow live Testnet terminé, ni Gate E.

Le runtime exige un venv dédié sans `PYTHONHOME`, `PYTHONPATH`,
`PYTHONUSERBASE`, user-site ou customization module. Les dépendances externes sont
installées depuis `requirements-external.lock` avec hashes et wheelhouse offline ;
le build utilise `requirements-build.lock`. HyperLab n'est jamais résolu par son
nom depuis un index : les wheels racine et service sont construits localement puis
installés avec `--no-index --no-deps`. Le rapport logiciel lie le chemin Python,
les octets installés, les locks, le worktree et les artefacts ; le même venv doit
émettre les preuves puis exécuter le runtime. Les sources de production sous
`hyperlab_testnet` n'importent ni `hyperlab.paper` ni `hyperlab.data` ; leurs
primitives canoniques sont locales. Le wheel racine reste uniquement la source
locale revue de l'autorisation d'environnement et des artefacts explicitement
hashés par l'identité de build.

Le venv opérateur minimal invoque la validation, mais les gates Ruff/mypy/pytest et
manifest/release utilisent le Python revu au chemin fixe `.venv` racine. Le rapport
lie par chemin et SHA-256 les deux exécutables ; aucun outil de développement n'est
ajouté au runtime Testnet pour faire passer les gates.

Le plan de contrôle interprocessus est fixé à
`%ProgramData%\HyperLab\TestnetExecutor\control-v1` et n'a aucun override CLI. La
feuille doit être pré-provisionnée par un administrateur pour le SID exact du compte
d'exécution ; le service ne la crée pas. La même politique s'applique séparément à
`HyperLab`, `TestnetExecutor` et `control-v1`. Propriétaire et droits de contrôle sont
limités au SID opérateur, à `SYSTEM` et aux Administrateurs, avec DACL non nulle et
ACE allow/deny simples. Tout composant reparse/non-directory, DACL illisible ou
droit d'écriture accordé à un autre SID bloque le store. Le registre durable porte
lease/rate/send-gate/kill à portée compte et nonce à portée API wallet : le supprimer
ou le recréer contournerait ces contrôles et est interdit.

La rate ledger conserve définitivement chaque identité ordinaire
`SUBMIT/CANCEL/REPLACE` afin qu'un autre processus ou une autre base ne puisse la
rejouer. Sa capacité compilée est `100000`, sans éviction/reset ; le compte d'usage
est audité à chaque action. L'exploitation doit s'arrêter largement avant la
capacité. `SCHEDULE_CANCEL` reste une voie protectrice exemptée.

Les douze limites Testnet ont des plafonds compilés égaux aux défauts documentés ;
la configuration peut uniquement les abaisser. Le parseur décimal exact limite à
64 les chiffres du coefficient, la valeur absolue de l'exposant et celle de
l'exposant ajusté avant tout formatage, afin qu'une notation compacte hostile ne
déclenche pas une allocation géante.

Les seuls noms de variables secrets admis sont
`HYPERLAB_TESTNET_PRIVATE_KEY`, `HYPERLAB_TESTNET_ACCOUNT_ADDRESS` et
`HYPERLAB_TESTNET_API_WALLET_ADDRESS`. La clé doit dériver l'adresse API wallet
configurée ; l'adresse principale et l'API wallet sont distinctes. Les valeurs ne
sont jamais stockées dans Git, `.env`, la configuration, SQLite, les logs, le
dashboard ou les preuves. Un namespace générique, Paper, micro-mainnet ou Mainnet
ambigu bloque le chargement. L'opérateur injecte les variables depuis un
gestionnaire de secrets local dans le seul processus exécuteur. Le compte Testnet
doit avoir le rôle exact `user` et exactement une API wallet active configurée ;
vault, sous-compte, deuxième agent actif et opération principale simultanée sont
refusés.

Le kill ne doit pas être confondu avec une preuve d'annulation. La commande exige
`--database --run-id --confirm TESTNET-KILL` et persiste d'abord un latch compte
irréversible.
Sans bundle complet, si le lease appartient au runtime, ou si `scheduleCancel`
n'est pas confirmé, elle sort avec le code `3` et conserve `KILLED`. Même un
`DEADMAN_ARMED` confirmé ne liquide pas la position et ne prouve pas que chaque
ordre est déjà annulé ; la vérification venue et la révocation hors bande restent
requises en cas de dégradation.

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
Le service Phase 13 ne contient lui non plus aucune commande, route, configuration
ou autorisation Mainnet et ne peut activer le flag argent réel.

## Menaces à tester

- crash avant/après commit atomique d'un événement paper ;
- événement live ou paper dupliqué avec contenu identique ou contradictoire ;
- journal modifié ou tronqué et projection obsolète ;
- divergence de replay à configuration/seed identiques ;
- contournement du moteur paper par une stratégie ou le dashboard ;
- apparition d'un import privé, d'une clé, d'un signer ou d'une route d'ordre dans
  le paquet racine/Umbrel ;
- fuite ou sérialisation d'un secret dans le service Testnet ;
- réponse réseau perdue après un ordre Testnet potentiellement accepté ;
- événement WebSocket dupliqué ou manquant ;
- clock drift ;
- données obsolètes ;
- redémarrage avec positions ouvertes ;
- corruption locale ;
- injection via dashboard ;
- dépendance compromise ;
- substitution d'un paquet d'index `hyperlab` ou wheelhouse modifié ;
- validation exécutée dans un Python différent de celui du runtime ;
- conteneur escaladant vers l'hôte ;
- erreur humaine de réseau testnet/mainnet.
