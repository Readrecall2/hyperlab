# Paper engine — Phase 12

## Statut et frontière de sécurité

Le runtime Phase 12 peut exécuter une stratégie figée sur un flux public live, mais tous les
ordres, acknowledgements, rejets, fills et cancels restent **simulés localement**.
Le package paper ne contient ni clé, ni wallet, ni signature, ni client privé, ni
transport d'ordre et ne peut pas envoyer, modifier ou annuler un ordre sur une
venue. `EMERGENCY_FLATTEN` signifie donc « réduction simulée urgente » et ne
constitue jamais une action exchange.

La conformité technique du moteur ne vaut pas validation économique. Dans ce
checkout, le statut de promotion économique reste **`BLOCKED`** : aucune fenêtre
forward paper de 6 à 8 semaines, aucun nombre suffisant de cycles et aucune période
de 14 jours sans incident critique ne sont encore démontrés. Les prérequis de
données et de calibration des Phases 10 et 11 restent eux aussi fermés. Un run sur
fixture, un modèle `SYNTHETIC` ou une hypothèse `UNCALIBRATED` sert uniquement à
tester le logiciel.

Le runtime continu et le sous-groupe CLI `paper` sont implémentés, avec reprise,
réconciliation avant admission, timers et arrêt propre. Cependant, le registre
statique `config_hash → stratégie + adaptateur de source publique` est
intentionnellement vide dans ce checkout : aucune stratégie Phase 10/11 et aucun
adaptateur live n'ont encore satisfait leurs prérequis. `paper run` échoue donc
fermé avant de créer un store. `paper status`, `paper replay` et
`paper reconcile` restent disponibles pour les stores de démonstration et de test.

## Périmètre

Le moteur paper doit fournir :

- une machine à états persistante et rejouable ;
- une seule voie d'acceptation pour toutes les décisions de stratégie ;
- des identifiants déterministes et des événements idempotents ;
- un cycle d'ordre simulé complet, y compris rejets, non-fills, fills partiels,
  IOC, expirations, cancels et délais entre jambes ;
- un ledger cash/positions/frais/PnL exactement réconcilié ;
- des limites de risque vérifiées avant chaque acceptation simulée ;
- un redémarrage fail-closed et un replay déterministe ;
- une projection de statut et un dashboard strictement read-only ;
- des alertes persistées et auditables.

Sont hors périmètre : authentification, données privées de compte, appels wallet,
signature, nonce exchange, dépôt, retrait, transfert réel, ordre testnet ou mainnet
et promotion automatique vers une phase suivante.

## Configuration figée et provenance

Chaque run commence avec une configuration immuable. Son hash canonique couvre au
minimum :

- l'identifiant et la version de la stratégie ;
- tous ses paramètres et son univers autorisé ;
- la liste figée des instruments dont chaque canal doit rester frais ;
- le hash de l'artefact prouvant les Gates B/C applicables ;
- les limites de risque ;
- le capital paper initial et les règles de valorisation ;
- les modèles de frais, spread, slippage, profondeur, latence et fill ;
- les identifiants et hashes des artefacts de calibration ;
- le seed déterministe ;
- les versions de schéma et de code nécessaires au replay.

Une modification de paramètre, de limite, de seed ou d'artefact crée un **nouveau
run** et un nouveau hash. Elle ne peut pas réécrire un run actif ni prolonger sa
fenêtre de validation. Les stratégies ne reçoivent aucun accès direct au store ou
au simulateur : elles produisent une décision, puis le paper engine applique le
risque et le cycle d'exécution.

Les frais ne sont jamais lus depuis un compte. Une configuration `CALIBRATED`
doit embarquer un `CostSchedule` point-in-time ; deux scalaires globaux ne rendent
qu'un run de démonstration possible. La source requise est un artefact
public, versionné et hashé contenant la grille publiée par la venue, ses intervalles
d'effet, l'URL et la date de collecte, le palier retenu et les hypothèses. En
l'absence de preuve publique d'une remise, le run choisit le palier public
conservateur applicable et n'invente aucune réduction liée au compte. Un fichier
placeholder reste `UNCALIBRATED` même s'il possède un hash : le hash prouve son
identité, pas sa qualité.

## Machine à états

Le chemin nominal est :

```text
FLAT
  → ENTRY_PLANNED
  → LEG_1_PENDING
  → HEDGE_PENDING
  → HEDGED
  → EXIT_PLANNED
  → EXIT_PENDING
  → FLAT
```

Pour une stratégie à une seule jambe, `HEDGED` signifie que l'exposition cible
acceptée est établie ; le passage par `HEDGE_PENDING` peut être sans objet. Les
états de protection peuvent interrompre le chemin nominal :

| État | Invariant |
|---|---|
| `FLAT` | aucune exposition et aucun reliquat d'ordre simulé ouvert |
| `ENTRY_PLANNED` | décision d'entrée persistée, pas encore acceptée comme ordre |
| `LEG_1_PENDING` | première jambe acceptée, avec reliquat explicite tant qu'elle n'est pas terminale |
| `HEDGE_PENDING` | exposition transitoire connue et hedge simulé encore incomplet |
| `HEDGED` | portefeuille cible effectivement rempli, jamais simplement demandé |
| `EXIT_PLANNED` | décision de sortie persistée et contrôlée par le risque |
| `EXIT_PENDING` | sortie acceptée mais possiblement partielle ou non remplie |
| `PAUSED` | aucune nouvelle entrée ; les expositions et reliquats restent suivis |
| `REDUCE_ONLY` | seules les décisions réduisant l'exposition absolue sont acceptables |
| `MANUAL_REVIEW` | incohérence ou ambiguïté ; aucune progression automatique risquée |
| `EMERGENCY_FLATTEN` | tentative simulée immédiate de réduction, avec non-fill et reliquat possibles |

Une protection ne peut jamais être utilisée pour augmenter une exposition. Un
fill partiel ne permet pas de sauter vers `HEDGED` ou `FLAT`. Une sortie d'un état
de sécurité est elle-même un événement audité ; elle n'est pas une modification
directe de la ligne d'état.

## Journal, identité et idempotence

SQLite est la source de vérité opérationnelle du paper engine. Le journal est
append-only, ordonné et chaîné par hashes. Une transaction atomique persiste
l'événement accepté, sa transition, ses écritures de ledger et les données dérivées
nécessaires. La vue structurée du dashboard est reconstruite depuis SQLite et
n'est jamais une seconde autorité.

Les identifiants sont des SHA-256 de sérialisations canoniques avec un domaine
explicite. Le contrat logique est :

```text
run_id      = sha256("paper_run"      + frozen_config_hash)
decision_id = sha256("paper_decision" + run_id + market_event_id + action + ordinal)
order_id    = sha256("paper_order"    + run_id + decision_id + ordinal + intention canonique)
event_id    = sha256("paper_event"    + run_id + type + causalité + corrélation + contenu + ordinal)
```

Les séparateurs, clés triées, unités, timestamps UTC et représentation numérique
font partie du format canonique. Un même événement rejoué produit le même ID et une
seconde ingestion devient un no-op vérifié. La réutilisation d'un ID avec un contenu
différent est une collision logique : le moteur échoue fermé et passe en
`MANUAL_REVIEW`.

À l'ouverture, le store vérifie la chaîne, l'ordre, l'unicité, le head attendu et la
cohérence des projections. Une modification, une troncature, un trou ou une
transition illégale interdit toute nouvelle entrée.

## Cycle d'un ordre simulé

Chaque ordre conserve le lien complet suivant :

```text
décision de stratégie
→ contrôle de configuration et fraîcheur
→ contrôle de risque pré-acceptation
→ ordre simulé déterministe
→ ack ou reject simulé
→ activation après latence
→ zéro, un ou plusieurs fills partiels
→ fill complet, cancel, expiration ou reliquat
→ position, cash, frais et PnL
```

Les sémantiques durcies de la Phase 04 sont réutilisées lorsque leur granularité
est applicable : profondeur exécutable, spread, slippage dépendant de la
participation, non-fill maker, fill partiel, timeout, IOC, délai entre jambes,
frais et pénalité d'urgence. Un IOC peut lui aussi être partiel ou manqué. Une
entrée maker manquée n'est pas poursuivie implicitement en taker ; un IOC d'urgence
sert seulement à réduire une exposition ou traiter une jambe déjà ouverte.

Un fill n'existe que si le modèle figé le produit à partir d'une observation
éligible reçue avant la décision et de son seed. Le mid-price seul ne devient pas
un prix exécutable. Une donnée stale, un gap non réconcilié ou une profondeur
absente alors qu'elle est requise bloque l'acceptation au lieu de fabriquer un
fill. Une latence ou un fill model non calibré reste utilisable uniquement pour un
run `DEMO` non promouvable ; il bloque une configuration `VALIDATION` et Gate D.
La règle de coûts point-in-time est contrôlée avant acceptation puis à l'instant du
fill ; si son intervalle expire entre les deux, l'ordre devient terminal, une
alerte critique est persistée et le run passe en `PAUSED`.

## Ledger et réconciliation

Chaque mutation économique écrit les quantités, le prix simulé, le notionnel, le
cash, les frais signés, le slippage, le funding et le PnL par composante. Les valeurs
sont sérialisées sous forme décimale canonique, sans tolérance flottante implicite.
Une reconstruction
indépendante depuis les événements doit retrouver exactement :

- les quantités et reliquats de chaque ordre ;
- la position par venue/instrument ;
- le cash et les frais cumulés ;
- le PnL réalisé, non réalisé et net ;
- l'equity et les expositions brute et nette.

Le rapprochement compare le ledger reconstruit, les projections persistées et le
snapshot publié. Toute différence, y compris après crash, bloque les entrées et
force `MANUAL_REVIEW`. Une tolérance flottante implicite ou un ajustement de cash
« pour faire tomber » l'identité est interdit.

## Risque avant acceptation

Avant tout ack simulé, le moteur vérifie au minimum :

- appartenance de la décision au run et à la stratégie figés ;
- hash de configuration identique au run actif ;
- qualité, ordre causal et fraîcheur des données ;
- prix positif et taille valide ;
- notionnel par ordre et par instrument ;
- expositions brute et nette projetées, ordres actifs compris ;
- perte journalière et drawdown ;
- délai maximal d'une jambe non couverte ;
- compatibilité avec `PAUSED`, `REDUCE_ONLY` et les états de revue.

La profondeur et la quantité publique disponible sont ensuite consommées par le
modèle de fill partagé entre les ordres d'un même événement ; elles ne sont jamais
inventées pour forcer un remplissage.

Le contrôle porte sur l'exposition **projetée après fill**, pas seulement sur la
position courante. Un rejet de risque est un événement terminal traçable. Aucune
stratégie ne peut écrire directement un fill, une position ou le cash.

## Crash, redémarrage et replay

Au redémarrage, le moteur :

1. ouvre le store sans accepter de nouvelle décision ;
2. vérifie la chaîne et le head ;
3. rejoue les événements depuis le début ou un checkpoint vérifié ;
4. reconstruit ordres, reliquats, positions, cash, frais et état ;
5. rapproche cette reconstruction avec les projections persistées ;
6. reprend seulement si l'identité est exacte et les données live sont fraîches.

Un état `LEG_1_PENDING`, `HEDGE_PENDING`, `EXIT_PENDING` ou
`EMERGENCY_FLATTEN` ne disparaît pas lors d'un restart. Les mêmes observations,
configuration, version du moteur et seed doivent reproduire octet pour octet les
décisions, IDs, événements, fills, écritures de ledger et hashes finaux. La
commande de replay réexécute l'inbox canonique dans un store isolé puis compare
les sorties ; elle ne se contente pas de relire la projection. Un événement reçu
de nouveau après le restart reste idempotent.

Les tests de crash couvrent au minimum : interruption autour d'une transaction,
restart après fill partiel, hedge en attente, cancel en attente, événement dupliqué,
projection obsolète, chaîne modifiée ou tronquée et lecture dashboard sur un store
corrompu.

## Dashboard et alertes

L'API dashboard ouvre SQLite en lecture seule et vérifie l'intégrité sans modifier
le store. Elle expose le mode `PAPER ONLY`, `orders_enabled=false`, les heads du
journal, la projection (état, ordres, positions, cash, frais, PnL et fraîcheur) et
les alertes persistées. Gate D reste évaluée séparément depuis le store vérifié ;
le dashboard ne fabrique aucun statut économique. Il n'expose aucun bouton ou
endpoint permettant de soumettre, annuler, reprendre, changer un paramètre ou
acquitter silencieusement une alerte.

Les alertes sont elles-mêmes persistées. Le moteur couvre notamment corruption du
store ou conflit d'identité durable, échec de réconciliation, donnée stale ou gap,
perte journalière, drawdown et jambe non couverte au-delà du délai. Un replay lancé
en lecture seule signale sa divergence à l'appelant sans modifier le store source.
Le canal de notification ne reçoit aucun secret de trading et ne peut pas commander
le moteur.

## Gate de sortie Phase 12

La Gate D reste fermée tant que toutes les conditions suivantes ne sont pas prouvées
sur des artefacts reproductibles :

- stratégie et paramètres gelés avant le début ;
- prérequis économiques des Gates B et C satisfaits pour la stratégie concernée,
  avec artefact SHA-256 figé dans la configuration ;
- coûts, latence et fills `CALIBRATED` avec preuves publiques/versionnées/hashées ;
- cible de 6 à 8 semaines de paper live continu, avec minimum dur de 42 jours ;
- nombre de cycles suffisant préenregistré (30 à 50 pour une stratégie lente,
  davantage pour une stratégie rapide) ;
- 14 jours consécutifs sans incident critique ;
- exercices persistés de redémarrage, déconnexion, fill partiel et récupération
  après crash ; rejets, non-fills, cancels, IOC et jambes retardées couverts par la
  gate technique ;
- réconciliation exacte et replay déterministe ;
- résultat net positif sous coûts stressés, sans masquer les runs perdants.

La réussite de `ruff`, `mypy` et `pytest` valide la conformité technique, pas cette
gate économique. Une fixture ou une démo ne compte ni dans la durée, ni dans les
cycles, ni dans les 14 jours. Le franchissement de la Gate D exige une revue humaine
explicite ; il autorise au plus la création séparée d'un executor **testnet** en
Phase 13 et ne crée jamais de capacité de trading dans la Phase 12.

Le gate logiciel lit exclusivement le store autoritaire vérifié. Les cycles et
incidents viennent du journal ; le seuil de cycles, jamais inférieur à 30, est figé
dans la configuration. Chaque canal requis doit être frais et le run doit être dans
un état opérationnel résolu (`FLAT` ou `HEDGED`). Le résultat stressé référence le
head et la séquence du préfixe économique exact qu'il évalue ; tout événement
économique ultérieur invalide cette preuve jusqu'à un nouveau stress. La couverture
continue et les exercices de résilience sont eux aussi liés à des artefacts
SHA-256. Des booléens passés par l'appelant ou une projection détachée ne peuvent
pas produire `PASS`. Un run au-delà de huit semaines reste éligible : huit semaines
est une cible, pas une date d'expiration.

## Limites connues

- Un flux public ne révèle pas les acknowledgements, rejets ou fills privés d'un
  compte ; ils restent donc des sorties du modèle, clairement étiquetées.
- Un carnet agrégé ne prouve pas la position réelle dans la file ni la liquidité
  cachée.
- Les stratégies Phase 10/11 ne peuvent pas emprunter des hypothèses inachevées pour
  démarrer leur fenêtre paper.
- Le fonctionnement pendant plusieurs semaines et l'observation de suffisamment de
  régimes ne peuvent pas être remplacés par des tests accélérés.
- Aucun résultat Phase 12 ne constitue à lui seul une promesse de performance live.
