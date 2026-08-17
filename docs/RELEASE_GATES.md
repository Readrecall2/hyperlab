# Portes de validation et classes d'environnement

Ce document est le contrat normatif de préparation et d'autorisation HyperLab. Il
sépare la préparation technique d'un environnement sans argent réel de
l'autorisation explicite d'exposer du capital réel.

Un reçu de préparation est lié à une classe d'environnement, un but, une
configuration, une stratégie, un build, une source ou un endpoint et des limites
de risque. Le vérificateur exige l'égalité exacte de la classe et du but demandés.
Il n'existe ni ordre implicite entre les classes, ni conversion, ni héritage, ni
fallback permissif : un reçu `PAPER` ou `TESTNET` ne peut jamais autoriser
`MICRO_MAINNET` ou `MAINNET`. Une identité absente, inconnue, composite ou ambiguë
échoue fermée.

Un SHA-256 prouve l'identité des octets, pas leur sens. Chaque check requis doit
donc disposer d'un vérificateur sémantique compilé et versionné, lié exactement au
triplet environnement/but/check et au sujet (config, stratégie, build, source et
risque). Le profil et le reçu lient le jeu complet de ces vérificateurs. Un
vérificateur absent, une exception, un résultat autre que vrai ou un artefact
modifié pendant le contrôle bloque la décision ; un fichier fourni par l'appelant
avec `"status":"PASS"` ne suffit jamais.

| Classe | But unique | Capacité |
|---|---|---|
| `RESEARCH_REPLAY` | `RESEARCH_REPLAY` | calcul/replay inerte, aucun ordre envoyé à une venue |
| `PAPER` | `PAPER_RUNTIME` | ordres simulés localement uniquement |
| `TESTNET` | `TESTNET_EXECUTION` | ordres vers le Testnet isolé uniquement |
| `MICRO_MAINNET` | `MICRO_MAINNET_EXECUTION` | capital réel microscopique et borné |
| `MAINNET` | `MAINNET_EXECUTION` | capital réel, autorisation séparée |

Le `network=mainnet` d'une source publique ne constitue pas une identité
d'exécution. HyperLab 0.2.x conserve `HYPERLAB_MODE=readonly|research`, ne
contient aucun transport d'ordre réel et ne peut ni émettre ni consommer de reçu
d'exécution réelle ; il peut seulement évaluer la politique. Le reçu de release
Phase 15 prouve la provenance de l'image et ses scans ; il n'autorise aucun runtime
de trading.

La politique compilée s'inspecte sans écriture :

```powershell
.\.venv\Scripts\python.exe -m hyperlab gate-model requirements PAPER
.\.venv\Scripts\python.exe -m hyperlab gate-model check .\readiness.json --evidence-root .\evidence
```

`requirements` publie le profil exact et la couverture des vérificateurs
sémantiques compilés. `check` exige un manifeste JSON canonique, revérifie chaque
artefact byte-bound puis exécute le vérificateur exact ; il ne publie un reçu que
pour `READY` et sort avec le code 2 pour `BLOCKED`. Ces commandes ne démarrent
aucun runtime et n'écrivent aucun artefact. Le registre de vérificateurs est vide
dans ce build : de simples pseudo-preuves restent donc `BLOCKED`.

## Gate A — base logicielle / `RESEARCH_REPLAY`

- tests, lint et type-check passent ;
- démo synthétique reproductible et visiblement étiquetée ;
- aucun import d'exécuteur dans les couches recherche/backtest ;
- dashboard affiche `READ-ONLY` ;
- configuration, données et résultats sont identifiés et rejouables.

Cette gate autorise uniquement la recherche et le replay. Elle ne produit aucune
conclusion économique et aucune autorisation d'ordre.

## Gate B — données économiques

- au moins 30 jours de collecte propre pour les stratégies lentes ;
- rapport de trous et fraîcheur ;
- timestamps synchronisés ;
- données externes si la stratégie les exige.

Gate B peut progresser en parallèle de `PAPER` et `TESTNET`. Son absence ne
bloque pas leur préparation technique ; elle bloque toute promotion avec argent
réel.

## Gate C — validation économique

- plan train/validation/test hashé et révélation finale unique ;
- variantes, pertes et erreurs inscrites avant consultation du résultat ;
- walk-forward chronologique avec embargo et OOS non chevauchant ;
- données point-in-time, lifecycle et finalité disponibles ;
- coûts, profondeur, fills et latence calibrés sur des observations versionnées ;
- hashes des preuves de calibration présents et contenu/méthode/couverture audités
  (la présence d'un hash ne suffit pas) ;
- résultat économique mesuré après coûts réalistes, sans conditionner la conformité
  technique du moteur à un rendement positif ;
- stress ×2, maker et latence dégradés documentés ;
- pas de dépendance à un seul actif ou mois ;
- PnL attribué au mécanisme attendu ;
- avantage économique clair face au rendement passif.

Des données `SYNTHETIC` ou des hypothèses `UNCALIBRATED` laissent Gates B/C
fermées, quel que soit le rendement affiché. Elles peuvent néanmoins servir à la
préparation technique non autorisante de `PAPER` ou `TESTNET` si leur nature et
leurs limites sont explicites.

## Préparation `PAPER`

Un reçu `PAPER` / `PAPER_RUNTIME` peut être émis sans PASS Gate B, C ou D
si toutes les conditions techniques suivantes sont dérivées de preuves persistées :

- identité d'environnement exactement `PAPER` ;
- mode simulation-only prouvé : aucune clé, signature, donnée privée de compte,
  client d'exécution ou route permettant d'envoyer, modifier ou annuler un ordre ;
- source publique autorisée et schéma `MarketEvent` normalisé valide ;
- stratégie, paramètres, univers, limites, seed, source et versions figés/hashés ;
- coûts, latence et fills conservateurs et clairement étiquetés
  `CALIBRATED`, `UNCALIBRATED` ou `SYNTHETIC` ;
- une seule voie stratégie → risque → simulateur, sans écriture directe de fills ;
- identifiants déterministes, journal append-only, comptabilité exacte et replay ;
- crash recovery, reprise et réconciliation avant toute nouvelle décision ;
- snapshot/dashboard read-only et alertes persistées ;
- artefact lié au but `PAPER_RUNTIME` avec `authorizes_real_money=false`.

Cette règle n'est pas un bypass économique : un Paper non calibré reste
non-promouvable, mais il peut tester le logiciel et exercer l'instrumentation des
futures preuves Gate D. Son temps et ses cycles ne créditent jamais la fenêtre
qualifiante. Dans ce checkout, le registre runtime Paper
reste vide, aucun jeu de vérificateurs sémantiques candidat n'est compilé et aucune
stratégie + source candidate complète n'est inscrite ;
`paper run` demeure donc techniquement bloqué pour ces raisons, pas parce que
Gates B/C/D sont fermées.

## Gate D — preuve forward Paper pour argent réel

Gate D ne sert pas à démarrer `PAPER` ou `TESTNET`. Elle est une preuve obligatoire
pour `MICRO_MAINNET` et `MAINNET` et exige cumulativement :

- Gates B et C satisfaites pour la stratégie inscrite ;
- check `paper_readiness_receipt_bound` vrai : configuration et reçu exact
  `PAPER` / `PAPER_RUNTIME` figés avant la période ;
- modèles de coûts, latence et fills `CALIBRATED`, preuves auditées et grille de
  frais publique versionnée/hashée ;
- cible de 6 à 8 semaines de fonctionnement forward continu, minimum dur 42 jours ;
- seuil préenregistré d'au moins 30 cycles complets ;
- 14 jours consécutifs sans incident critique ;
- canaux requis frais, état terminal `FLAT` ou `HEDGED`, replay et réconciliation
  exacts ;
- redémarrages, déconnexions, rejets, non-fills, fills partiels, cancels, IOC et
  jambes retardées exercés ;
- résultat net positif sous coûts stressés, lié au head économique final, sans
  exclusion des runs perdants ;
- rapport reproductible liant code, configuration, données, journal et preuves.

Une fixture, un run accéléré ou une hypothèse non calibrée ne compte ni pour la
durée, ni pour les cycles, ni pour les 14 jours. Gate D reste actuellement
`BLOCKED`, mais ce statut n'interdit plus la préparation `PAPER` ou `TESTNET`.
Le temps et les cycles d'un run technique antérieur ne sont jamais repris
rétroactivement : la fenêtre qualifiante commence avec une nouvelle configuration
`VALIDATION` figée après PASS Gates B/C et calibration.

## Préparation `TESTNET`

Un reçu `TESTNET` / `TESTNET_EXECUTION` ne requiert ni rentabilité, ni Gates
B/C/D, ni campagne Paper de 42 jours. Il exige :

- service/version Phase 13 séparé et identité exactement `TESTNET` ;
- venv opérateur dédié, deux wheels locaux revus installés `--no-index --no-deps`
  et graphes build/runtime issus des locks hashés et du wheelhouse offline fixe ;
- endpoint, chain ID et namespace Testnet explicitement allowlistés, sans fallback
  Mainnet ;
- credentials Testnet dédiés et séparés, rôle compte exact `user` et exactement
  une API wallet active configurée ; aucune réutilisation implicite d'un credential
  Mainnet, vault ou sous-compte ;
- CLOID déterministes, machine à états d'ordres, post-only/IOC/reduce-only et
  cancel/replace explicitement testés ;
- réponses ambiguës recherchées/réconciliées, jamais renvoyées aveuglément ;
- positions et compte réconciliés exchange-first au démarrage et après restart ;
- douze limites avec défauts/plafonds compilés non relevables par configuration,
  décimaux à représentation bornée, pause/dead-man switch et audit complet ;
- tombstones account-global submit/cancel/replace sans éviction, capacité compilée
  `100000` surveillée bien avant saturation ; `scheduleCancel` protecteur exempt ;
- kill compte durable avant réseau, avec résultat DMS explicitement confirmé ou
  dégradé ; aucun `DEADMAN_ARMED` interprété comme cancel déjà appliqué ;
- échec fermé pour toute identité, URL, chain ID, credential ou réponse ambiguë.

Le checkout contient désormais le service séparé
`services/testnet-executor` 0.3.0.dev0 et les vérificateurs sémantiques compilés
pour ces quatorze checks. L'identité est exactement `TESTNET` /
`TESTNET_EXECUTION`, avec HTTP
`https://api.hyperliquid-testnet.xyz`, WebSocket
`wss://api.hyperliquid-testnet.xyz/ws` et credential namespace
`HYPERLAB_TESTNET`. Les preuves sont canoniques et liées au build, à la
configuration, à la source, à la stratégie et aux limites ; une étiquette `PASS`
non vérifiée ne produit jamais un reçu.

La chaîne d'autorité commence par `build-identity` puis
`validate-software`, invoqués dans le venv dédié qui exécutera le runtime. Les
gates de développement s'exécutent avec le Python revu au chemin fixe `.venv` de
la racine, distinct du Python opérateur minimal ; le rapport lie leurs chemins et
hashes. Il lie aussi
branche/baseline, inventaire worktree avant/après, Python/exécutables, locks,
wheelhouse, wheels, lints/type checks/tests, diff/conflict/manifest/release checks
et build isolé. `evidence` recharge ce rapport absolu et lie son identité/hash aux
quatorze preuves avant de créer manifest et reçu. Chaque commande online recharge
les cinq chemins `config/receipt/manifest/evidence-root/validation-report` ; le
seul reçu n'est pas une autorité suffisante.

Cette présence logicielle n'est pas une preuve d'exécution live. Avant le premier
preflight réel, l'opérateur doit encore créer une API wallet Testnet dédiée,
pré-provisionner `%ProgramData%\HyperLab\TestnetExecutor\control-v1` avec la DACL
restreinte du SID d'exécution appliquée à chacun des trois composants projet,
injecter ses credentials hors dépôt, produire le
rapport logiciel puis le reçu exact sur le commit validé et approuver la
configuration. Le service refuse un registre absent, reparse ou writable par un
SID tiers ; il ne l'auto-crée pas. Avant le premier ordre,
le preflight et la
réconciliation read-only doivent réussir, puis l'ordre minimal exige la
confirmation `TESTNET-ORDER`. Le workflow A–H et Gate E restent non observés dans
ce checkout. Voir [`TESTNET_EXECUTOR_PHASE13.md`](TESTNET_EXECUTOR_PHASE13.md).

## Gate E — preuve opérationnelle Testnet pour argent réel

Gate E est produite après des exercices Testnet réussis ; elle n'autorise pas le
démarrage de Testnet. Elle lie aux octets vérifiés les signatures, CLOID, cycles
complets d'ordre, annulations/cancel-replace, partial fills, réponses perdues,
doublons, perte WebSocket, restart, réconciliation, limites, kill dégradé et
dead-man switch confirmé/non confirmé. Une
revue humaine explicite de cette preuve est obligatoire avant tout capital réel.
Les tests locaux et transports simulés Phase 13 ne comptent pas comme Gate E.

## Autorisation `MICRO_MAINNET`

Un reçu `MICRO_MAINNET` / `MICRO_MAINNET_EXECUTION` exige exactement :

- PASS Gates B/C/D et preuve Gate E terminée, tous reliés avec vérification octet
  par octet à une même lignée figée de stratégie, modèle économique et risque ;
- configuration et reçu propres à `MICRO_MAINNET`, distincts de ceux de Testnet ;
  le reçu Testnet n'est jamais consommé comme autorisation micro-mainnet ;
- revue humaine explicite ;
- endpoint Mainnet explicite sans fallback, configuration signée et signer isolé ;
- secrets dédiés, révocables et hors dépôt/logs/dashboard ;
- réconciliation, audit append-only, kill switch/dead-man switch et révocation testés ;
- limites codées de notionnel, position, perte et drawdown impossibles à augmenter
  depuis la stratégie ;
- 100 à 300 USDC maximum, levier 1×, une seule stratégie et une seule paire/position ;
- aucune augmentation automatique.

HyperLab 0.2.x ne contient ni signer ni route Mainnet et reste incapable d'émettre
ou de consommer cette autorisation.

## Gate F — preuve micro-mainnet terminée

Gate F est produite après la campagne micro-mainnet. Elle lie les fills, le
slippage, les frais, les incidents, les limites, la réconciliation et les exercices
de kill/reprise observés pendant 4 à 8 semaines à la lignée et à la configuration
micro exactes. Elle n'autorise aucune hausse automatique et reste requise avant
`MAINNET`.

## `MAINNET`

`MAINNET` n'est pas une extension implicite de `MICRO_MAINNET`. Un reçu exact et
distinct `MAINNET` / `MAINNET_EXECUTION`, lié à la configuration et au capital
exacts, exige :

- Gates B/C/D/E et preuve Gate F micro-mainnet satisfaites ;
- deux confirmations humaines indépendantes ;
- nouvelle configuration signée, limites de capital/perte/drawdown explicites et
  signer Mainnet isolé ;
- réconciliation et kill switch revérifiés ;
- nouvelle période d'observation avant chaque palier, sans hausse automatique.

Aucun reçu `RESEARCH_REPLAY`, `PAPER`, `TESTNET` ou `MICRO_MAINNET` ne peut
être réutilisé pour ce but.
