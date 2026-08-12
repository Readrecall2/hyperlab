# Collecteur public Hyperliquid — Phase 02

Dernière vérification documentaire et SDK : 12 août 2026.

## Statut de la phase

Ce collecteur ne manipule que les API publiques Hyperliquid. Il amorce un état
cohérent par REST, suit ensuite les flux WebSocket, rend chaque interruption
visible et rejoue les fixtures sans réseau.

La validation soak réelle de 24 heures est un **gate encore non certifié**.
L’existence de la commande, des métriques et des tests déterministes ne vaut pas
preuve de stabilité pendant 24 heures. La phase ne pourra être déclarée terminée
qu’après exécution et archivage du protocole décrit plus bas.

## Frontière de sécurité

- aucun wallet, aucune adresse utilisateur configurée et aucun secret ne sont
  nécessaires ;
- `hyperliquid.exchange.Exchange` est interdit à l’import comme à
  l’instanciation ;
- aucune signature, création, modification ou annulation d’ordre n’existe dans
  ce module ;
- le SDK officiel n’est utilisé que par `hyperliquid.info.Info` pour les appels
  REST publics, avec `skip_ws=True` ;
- le transport WebSocket est public et supervisé localement afin de contrôler
  les acquittements, heartbeats, reconnexions et resynchronisations ;
- le mode replay n’importe ni ne construit aucun client réseau.

Les messages publics `trades` peuvent eux-mêmes contenir des adresses de
participants publiées par l’API. Le collecteur ne demande jamais l’adresse de
l’opérateur. Les fixtures versionnées anonymisent ces champs et les identifiants
opaques qui pourraient permettre un rapprochement.

Un contrôle de sécurité minimal doit rester vert :

~~~powershell
rg -n "hyperliquid\.exchange|from hyperliquid import Exchange|Exchange\(" src tests
~~~

Le résultat attendu est vide.

## Architecture et cycle de vie

Le chemin nominal est :

~~~text
Premier démarrage :
BOOTSTRAPPING REST -> CONNECTING -> SUBSCRIBING -> tous les ACK -> LIVE

Après une coupure :
gap coverage_unknown -> BACKOFF -> RESYNCING REST (préconnexion)
                                 -> CONNECTING -> SUBSCRIBING -> tous les ACK -> LIVE

Pendant LIVE :
WebSocket continu <-> refresh REST périodique concurrent -> fusion/déduplication
~~~

### 1. Bootstrap REST

Avant d’ouvrir le premier WebSocket, le collecteur récupère :

- `metaAndAssetCtxs` pour les métadonnées et contextes perp ;
- `spotMetaAndAssetCtxs` pour les métadonnées et contextes spot ;
- `fundingHistory` sur la fenêtre configurée pour les perps ;
- `candleSnapshot` pour chaque intervalle demandé ;
- `l2Book` pour un état initial du carnet.

Les listes spot de métadonnées et de contextes ne sont pas supposées
positionnellement alignées. Les contextes sont associés par leur champ `coin`
et les marchés spot conservent leur identifiant source, par exemple
`PURR/USDC` ou `@107`. Une absence ou une ambiguïté provoque une erreur visible ;
aucune valeur de prix ou de volume n’est inventée.

### 2. Abonnements WebSocket

Pour chaque actif, les abonnements publics sont :

- `activeAssetCtx` : mark, oracle, mid, funding courant, open interest et
  volumes ;
- `bbo` : meilleur bid et meilleur ask ;
- `l2Book` : carnet L2 ;
- `trades` : transactions publiques ;
- `candle` : un abonnement par intervalle demandé.

Le collecteur ne passe pas en `LIVE` sur la seule ouverture TCP. Il attend un
`subscriptionResponse` correspondant à chaque abonnement. Un acquittement
manquant au-delà du délai autorisé force une reconnexion.

### 3. Resynchronisation obligatoire

Après une interruption et le backoff, le collecteur exécute la
resynchronisation REST **avant d’ouvrir le WebSocket de remplacement**. Les
événements `resync_start`/`resync_complete` et le flush du rattrapage précèdent
donc `CONNECTING`. Une fois le socket ouvert, la file des acquittements est la
seule porte restante : tous les ACK reçus font passer directement de
`SUBSCRIBING` à `LIVE`, sans second bootstrap REST post-connexion.

Cette resynchronisation restaure les états courants (contexte, candles et
carnet), mais ne prouve pas que tous les trades survenus pendant la coupure ont
été récupérés. Hyperliquid ne fournit pas de curseur public de reprise pour ces
abonnements. Chaque coupure produit donc un événement de connexion et un gap
`coverage_unknown` ; il est interdit de convertir ce gap en continuité supposée.

### 4. Refresh REST périodique concurrent

En `LIVE`, un worker REST unique rafraîchit périodiquement les métadonnées,
contextes, historiques de funding et candles sur une fenêtre de chevauchement.
Le thread de lecture WebSocket continue pendant ces appels ; le résultat n’est
fusionné dans le sink que lorsqu’il est disponible. Le refresh périodique
n’ajoute pas un second snapshot L2, déjà couvert par le flux WebSocket. Une
erreur REST reste visible dans `runtime_status.json` et produit un gap
`coverage_unknown`; elle n’est jamais masquée comme une collecte complète.

La déduplication persistante supprime les observations historiques strictement
identiques entre bootstrap, resynchronisation, refresh et redémarrage, tout en
conservant une correction dont le contenu source diffère.

### 5. Heartbeat, stale et backoff

Le heartbeat applicatif envoie `{"method":"ping"}` et attend
`{"channel":"pong"}`. La documentation Hyperliquid prévoit une fermeture après
60 secondes d’inactivité et recommande ce ping applicatif. Les valeurs par
défaut HyperLab sont :

| Paramètre | Défaut |
|---|---:|
| intervalle ping | 20 s |
| délai maximal sans pong | 45 s |
| seuil stale critique | 30 s |
| backoff initial | 1 s |
| backoff maximal | 30 s |
| jitter symétrique | 20 % |
| flush temporel | 5 s |
| batch | 500 lignes |
| capacité mémoire | 10 000 lignes |

Le backoff est exponentiel, borné et jitteré. `activeAssetCtx` et `l2Book`
sont des flux critiques :
leur dépassement du seuil stale ferme la connexion et déclenche le même chemin
de reprise. La file bornée échoue explicitement lorsqu’elle est pleine ; elle ne
supprime pas silencieusement une ligne.


Le compteur exponentiel n’est remis à zéro qu’après 60 secondes continues en
état `LIVE`, afin qu’une suite de connexions instables reste effectivement
bornée. Les attentes de backoff et de limite de connexions sont interruptibles.
### 6. Arrêt coopératif et délai de grâce

La CLI intercepte `SIGINT` et `SIGTERM` pour demander un arrêt coopératif :
sortie de la boucle, fermeture du socket, flush final, publication du dernier
`runtime_status.json`, fermeture du sink et du client REST. Les compositions
Docker locale et Umbrel accordent `stop_grace_period: 30s`; leur configuration
REST par défaut utilise un timeout de 15 secondes. L’annulation empêche toute
nouvelle page ou requête et interrompt l’attente du rate limiter ; une requête
HTTP déjà en vol reste néanmoins bornée par son timeout. Si un processus ne
termine pas dans cette fenêtre, l’orchestrateur peut encore le tuer : la preuve
d’un arrêt propre reste donc le statut final et la validation des manifestes,
pas la seule réception du signal.

## SDK officiel épinglé

La dépendance de production est exactement
`hyperliquid-python-sdk==0.24.0`. Les signatures suivantes ont été inspectées
dans cette distribution installée ; elles ne sont pas déduites d’exemples :

~~~python
Info(
    base_url: Optional[str] = None,
    skip_ws: Optional[bool] = False,
    meta: Optional[Meta] = None,
    spot_meta: Optional[SpotMeta] = None,
    perp_dexs: Optional[List[str]] = None,
    timeout: Optional[float] = None,
)

Info.meta(self, dex: str = "") -> Meta
Info.meta_and_asset_ctxs(self) -> Any
Info.spot_meta(self) -> SpotMeta
Info.spot_meta_and_asset_ctxs(self) -> Tuple[SpotMeta, List[SpotAssetCtx]]
Info.funding_history(
    self, name: str, startTime: int, endTime: Optional[int] = None
) -> Any
Info.candles_snapshot(
    self, name: str, interval: str, startTime: int, endTime: int
) -> Any
Info.l2_snapshot(self, name: str) -> Any
Info.all_mids(self, dex: str = "") -> Any
Info.subscribe(self, subscription: Subscription, callback: Callable[[Any], None]) -> int
Info.unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool
Info.disconnect_websocket(self)
~~~

Deux conséquences sont importantes :

1. `meta_and_asset_ctxs` n’accepte pas de paramètre `dex` dans cette version.
2. `l2_snapshot` n’accepte que `name`. Quand les champs REST documentés
   `nSigFigs` et `mantissa` sont nécessaires, l’adaptateur envoie un payload
   `info` public explicite au lieu de prétendre que le SDK expose ces arguments.

Le gestionnaire WebSocket du SDK 0.24.0 ne fournit pas les hooks nécessaires
pour observer fermeture/erreur, superviser le backoff et imposer une
resynchronisation. C’est la raison du transport WebSocket dédié ; ce choix
n’autorise aucun endpoint d’échange.

- SDK officiel, tag 0.24.0 :
  https://github.com/hyperliquid-dex/hyperliquid-python-sdk/tree/0.24.0
- source de `Info` pour ce tag :
  https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/0.24.0/hyperliquid/info.py

## Sémantique des données

### Contextes de marché

Le contexte perp/spot normalise les valeurs présentes : mark, oracle, mid,
funding courant, open interest et volume de base/notionnel sur 24 heures. Un
notionnel dérivé est calculé avec des décimaux exacts et reste distinguable de
la valeur source. Les messages de contexte ne portent pas tous une heure source :
`received_time` décrit alors l’observation locale et ne doit pas être présenté
comme une heure d’échange.

`prevDayPx` est obligatoire dans les types officiels REST/WS, mais Hyperliquid
transmet pourtant `"0.0"` sur certains spots quand aucun prix de la veille
exploitable n'est disponible. Ce zero source reste distinct d'un champ absent
ou `null`, que le parseur convertit defensivement en `None`. L'invariant du lake
est donc `previous_day_price >= 0` si le champ existe. Les prix de marche
`mark_price`, `oracle_price` et `mid_price` gardent leur contrainte stricte
`> 0` lorsqu'ils existent ; un nombre negatif reste refuse.


`allMids` peut utiliser le dernier trade lorsque le carnet est vide selon la
documentation ; cette valeur n’est donc jamais promue silencieusement en « vrai
mid de carnet ».

### Funding historique

Les heures source sont conservées telles quelles. Comme les points horaires
peuvent être décalés de quelques millisecondes, la détection de trous raisonne
par bucket horaire avec une tolérance documentée, pas par égalité naïve aux
frontières UTC. La pagination déduplique les bornes inclusives et refuse une
page qui n’avance pas.

### Candles

Les candles REST et WebSocket conservent ouverture, clôture, OHLC, volume de
base et nombre de trades. Un volume quote absent de la source reste `null`.
Hyperliquid ne fournit pas ici de bit de finalité exploité par le collecteur :
`is_final` reste donc `null`. Chaque observation porte un `observation_id`; les
révisions successives d’une même candle sont conservées et ne sont jamais
finalisées, mutées ou supprimées sur la seule foi de l’horloge locale. Une
révision identique est dédupliquée, tandis qu’une correction de contenu reste
auditable. Les trous sont évalués selon l’intervalle fixe déclaré.

L’intervalle calendaire `1M` est explicitement refusé tant que le lake ne sait
pas modéliser des mois de durée variable. Les intervalles fixes jusqu’à `1w`
restent acceptés.

### BBO et L2

`l2Book` est un état complet du carnet, pas un delta. Chaque message devient un
`l2_snapshot` indépendant ; le collecteur ne reconstruit pas un carnet en
appliquant de faux deltas. L’endpoint REST retourne au plus 20 niveaux par côté.

`bbo` est aussi un état complet, envoyé seulement lorsque le BBO change. Un côté
peut être `null`. Dans ce cas le message wire est conservé et l’anomalie est
visible, sans fabriquer un prix ou une taille.

### Trades et séquences

`tid` est un identifiant/hash de trade, pas une séquence monotone. La clé stable
est construite avec `(time, coin, tid)`. Ni `tid`, ni l’ordre d’arrivée local, ni
un compteur synthétique ne sont utilisés comme séquence serveur.

Les flux `trades`, `bbo`, `l2Book`, `candle` et `activeAssetCtx` ne fournissent
pas de séquence serveur publique exploitable. `source_sequence` reste donc
`null`. Une séquence d’arrivée locale repart à 1 pour chaque
`(connection_id, connection_epoch)` et sert uniquement à prouver l’ordre des
frames observées dans cette connexion.

## Conservation wire et écriture

Chaque frame reçue produit un enregistrement `wire_message` avec :

- texte brut exact, indicateur JSON et hash SHA-256 ;
- canal, heure de réception UTC et identifiant de connexion ;
- epoch de connexion et séquence d’arrivée locale ;
- `source_sequence=null` lorsqu’Hyperliquid n’en fournit pas.

Le texte initial non JSON `Websocket connection established.`, les
`subscriptionResponse` et les `pong` sont conservés eux aussi. Un message
invalide reste auditable dans la couche wire même si aucune ligne normalisée
n’est produite.

Le sink regroupe les lignes par type, actif, jour et, lorsque nécessaire,
intervalle de candle ou cadence de funding. Il déduplique par clé primaire dans
un cache borné, trie selon le contrat de schéma, écrit un fichier Parquet
immuable adressé par son hash. Le fichier et son manifeste sont chacun publiés
de façon exclusive après écriture temporaire complète ; seuls les
manifestes validés rendent une partition consommable. Une collision n’écrase
jamais un artefact existant.

Pour les candles et le funding, `.collector-observations.sqlite3` est un index
de déduplication **dérivé**, jamais la source de vérité. Il indexe une clé
logique et le hash du contenu observé, se réconcilie avec les manifestes
Parquet validés au démarrage et peut être reconstruit depuis eux. Seule une
répétition consécutive identique est supprimée ; chaque transition de contenu,
y compris A → B → A, reste une nouvelle observation. Les clés naturelles
stables des trades sont elles aussi reconstruites et dédupliquées.

Un verrou exclusif impose un seul writer par lake. Après un crash entre la
publication d’un Parquet valide et celle de son manifeste, le manifeste est
reconstruit et publié atomiquement au redémarrage ; aucune ligne n’est cachée.

Cette atomicité vaut par fichier content-addressé et par manifeste, non comme
transaction globale entre plusieurs partitions ou groupes. Les révisions de
candle sont écrites comme observations immuables ; aucune ligne n’est promue
« finale » par l’horloge.

## Métriques et trous

`data/runtime_status.json` est publié atomiquement et expose au minimum :

- état, epoch, connexions, reconnexions et resynchronisations ;
- messages reçus, lignes normalisées, problèmes de normalisation ;
- batches/lignes écrits et plus haut niveau de la file ;
- âge du dernier message et du dernier pong ;
- âge par `channel:asset[:interval]` et liste des flux stale ;
- backoff courant, compteurs ping/pong et gaps visibles.

Les manifestes de partitions complètent ce statut par bornes temporelles,
hashes, doublons, ordre, trous détectables et qualité. Trois catégories doivent
rester distinctes :

- `sequence_gap` : uniquement lorsqu’une vraie séquence source existe ;
- `arrival_gap` : discontinuité du compteur local dans un même epoch ;
- `coverage_unknown` : fenêtre de coupure qu’aucun curseur source ne permet de
  certifier.

L’absence de `sequence_gap` n’est jamais interprétée comme une preuve
d’exhaustivité quand `source_sequence` est absent.

## Limites API et budget

Les limites officielles sont appliquées comme contraintes de dimensionnement,
pas comme objectifs à saturer :

- 1 200 unités de poids REST par minute et par IP ;
- poids 20 pour la plupart des requêtes `info` ;
- poids 2 pour `l2Book` et `allMids` ;
- coût additionnel selon le nombre de points retournés : par 20 pour
  `fundingHistory` et par 60 pour `candleSnapshot` ;
- 10 connexions WebSocket simultanées, 30 nouvelles connexions par minute,
  1 000 abonnements et 2 000 messages envoyés par minute et par IP.

La configuration refuse plus de 1 000 abonnements sur une connexion. La
pagination REST est bornée, dédupliquée et séquentielle ; les reconnexions
utilisent le backoff afin de ne pas transformer une panne en tempête d’appels.
Un limiteur glissant réserve le poids maximal documenté de chaque page dans le
budget de 1 200 unités/minute. La pagination candle progresse vers le passé car
l’endpoint retourne les 5 000 points les plus récents de la fenêtre demandée.

Le collecteur refuse `1M` : le serveur connaît cet intervalle, mais sa durée
calendaire variable n’est pas encore représentable par le contrat de trous du
lake.

## Commandes

Inspecter d’abord le contrat réellement installé :

~~~powershell
hyperlab collect --help
hyperlab replay --help
~~~

Collecte continue publique BTC/ETH :

~~~powershell
hyperlab collect --network mainnet --assets BTC,ETH --candle-intervals 1m --duration-seconds 0 --max-messages 0 --batch-size 500 --history-lookback-hours 24
~~~

`0` signifie « sans limite » pour la durée ou le nombre de messages selon
l’option. Une exécution bornée doit préciser au moins une limite.

`--candle-intervals 1M` échoue volontairement à la validation de configuration ;
utiliser un ou plusieurs intervalles fixes séparés par des virgules.

Replay local sans réseau :

~~~powershell
hyperlab replay tests/fixtures/hyperliquid/replay --output data/replay-lake
~~~

Les fichiers sont lus dans un ordre stable, l’horloge est injectée et le même
jeu de fixtures doit produire deux fois les mêmes lignes canoniques et les mêmes
hashes de contenu. Le test interdit explicitement la création d’un socket. Les
fixtures issues de messages réels indiquent leur provenance et leurs champs
anonymisés ; une fixture documentaire ou dérivée doit porter cette étiquette et
ne doit pas être présentée comme une capture live.

## Protocole de validation soak 24 heures

### Préparation

1. Utiliser un dossier de données neuf et dédié à ce run, sans supprimer les
   preuves d’un run précédent.
2. Archiver la révision Git, `python --version`, la sortie de
   `python -m pip freeze` et son SHA-256, la version
   `hyperliquid-python-sdk==0.24.0`, l’OS et l’heure UTC de départ.
3. Lancer `ruff check .`, `mypy src/hyperlab` et `pytest` avec succès.
4. Vérifier le test replay deux fois et archiver les hashes canoniques.
5. Choisir à l’avance les deux fenêtres de coupure contrôlée et les seuils
   mémoire ; ne pas les adapter après observation.

`MANIFEST_SHA256.txt` couvre tous les fichiers de livraison sauf lui-même. Les
chemins sont triés et les fichiers texte sont hachés après normalisation
canonique des fins de ligne `CRLF` vers `LF`, afin que Windows et Linux
produisent le même manifeste.

Commande nominale :

~~~powershell
hyperlab collect --network mainnet --assets BTC,ETH --candle-intervals 1m --duration-seconds 86400 --max-messages 0 --batch-size 500 --history-lookback-hours 24
~~~

### Observations à archiver

Échantillonner au moins chaque minute :

- RSS/Private Bytes du processus, CPU, descripteurs/handles et taille disque ;
- état, epoch, file courante/maximum, messages, batches et erreurs ;
- âge de réception, âge du pong et âges de chaque flux critique ;
- manifestes créés, hashes, trous, doublons et `coverage_unknown` ;
- heure et durée exactes des coupures contrôlées.

Effectuer au moins deux coupures réseau contrôlées, dont une supérieure au délai
pong, puis rétablir le réseau. Chaque coupure doit laisser les preuves
`disconnect/gap -> backoff -> resync REST préconnexion -> connect ->
subscriptions acquittées -> LIVE` avec un nouvel epoch. Ne jamais modifier
l’horloge système pendant le run.

### Gate de réussite

Le rapport est accepté seulement si toutes les conditions sont vraies :

- durée observée d’au moins 24 heures et arrêt propre ;
- aucun crash, aucune partition partielle référencée et tous les
  hashes/manifestes valides ;
- aucune suppression silencieuse : file sous sa capacité, flush final vide et
  compteurs conciliés ;
- aucun stale silencieux ; tout stale provoque une transition et une preuve de
  reprise ;
- toute coupure possède un `coverage_unknown` et une resynchronisation complète
  avant `LIVE` ;
- séquences d’arrivée contiguës dans chaque epoch, et tous les trous funding ou
  candle détectables sont rapportés ;
- deux replays donnent des lignes canoniques byte-identiques ;
- après une heure de chauffe, la mémoire ne présente ni croissance monotone ni
  pente durable positive. À titre de seuil reproductible, la médiane RSS des
  30 dernières minutes ne doit pas dépasser celle des 30 minutes suivant la
  chauffe de plus de `max(64 MiB, 15 %)`, et la pente sur les 12 dernières
  heures ne doit pas dépasser 1 MiB/h. Un profil monotone échoue même s’il reste
  sous ce plafond.

Le rapport doit inclure les données brutes de monitoring, les commandes, les
logs, les manifestes et une conclusion explicite. Jusqu’à présence de cette
preuve dans le dépôt, le statut demeure :

> **SOAK 24 H : NON CERTIFIÉ — VALIDATION RÉELLE EN ATTENTE**

## Limites connues

- Aucun curseur public ne permet de récupérer avec certitude les trades d’une
  déconnexion ; la resynchronisation d’état ne comble pas cette limite.
- L’ordre d’arrivée local ne remplace pas une séquence serveur.
- Un contexte sans timestamp source ne permet de mesurer que la fraîcheur de
  réception.
- Un BBO one-sided est normalisé avec la paire prix/taille du côté absent à
  `null`; aucune valeur n’est fabriquée.
- `1M` reste refusé jusqu’à prise en charge explicite des intervalles calendaires
  variables.
- La publication est atomique par fichier content-addressé, pas transactionnelle
  pour un batch réparti entre plusieurs partitions.
- Le replay prouve le déterminisme du parseur sur les fixtures, pas la stabilité
  réseau ni mémoire pendant 24 heures.

## Références officielles

- Info endpoint :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Perpetuals info :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- Spot info :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
- WebSocket :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
- Abonnements WebSocket :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Timeouts et heartbeats :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats
- Limites :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- Identifiants d’actifs :
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids
- Historique :
  https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
