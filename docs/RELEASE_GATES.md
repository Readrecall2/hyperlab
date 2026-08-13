# Portes de validation

## Gate A — installation

- tests, lint et type-check passent ;
- démo synthétique reproductible ;
- aucun import d'exécuteur ;
- dashboard affiche `READ-ONLY`.

## Gate B — données

- au moins 30 jours de collecte propre pour les stratégies lentes ;
- rapport de trous et fraîcheur ;
- timestamps synchronisés ;
- données externes si la stratégie les exige.

## Gate C — backtest

- plan train/validation/test hashé et révélation finale unique ;
- variantes, pertes et erreurs inscrites avant consultation du résultat ;
- walk-forward chronologique avec embargo et OOS non chevauchant ;
- données point-in-time, lifecycle et finalité disponibles ;
- coûts, profondeur, fills et latence calibrés sur des observations versionnées ;
- hashes des preuves de calibration présents et contenu/méthode/couverture audités
  (la présence d'un hash ne suffit pas à elle seule) ;
- résultat économique mesuré après coûts réalistes, sans conditionner la conformité
  technique du moteur à un rendement positif ;
- stress ×2, maker et latence dégradés documentés ;
- pas de dépendance à un seul actif ou mois ;
- PnL attribué au mécanisme attendu ;
- avantage économique clair face au rendement passif.

La réussite des tests Phase 04 valide uniquement le cadre technique. Avec des
données `SYNTHETIC` ou des hypothèses `UNCALIBRATED`, les Gates B et C restent
fermées quel que soit le rendement affiché.

## Gate D — paper

### Préconditions techniques

- mode `PAPER ONLY` prouvé : aucune clé, signature, donnée privée de compte, client
  d'exécution ou route d'ordre réel ;
- toutes les stratégies passent par le même contrôle de risque et le même moteur de
  simulation ;
- configuration, paramètres, limites, seed et hashes de calibration figés avant la
  période ; toute modification démarre un nouveau run ;
- décisions, ordres, événements, fills et écritures de ledger ont des identifiants
  déterministes et sont idempotents ;
- journal persistant append-only, chaîne de hashes et replay déterministe vérifiés ;
- réconciliation exacte de chaque ordre, position, cash, frais et composante de PnL ;
- redémarrages, déconnexions, rejets, non-fills, fills partiels, cancels, IOC et
  jambes retardées testés ;
- snapshot et dashboard strictement read-only ; alertes critiques persistées ;
- `ruff check .`, `mypy src/hyperlab` et la suite `pytest` complète passent.

### Préconditions économiques

- Gates B et C déjà satisfaites pour la stratégie inscrite, avec artefact SHA-256
  figé dans le run ;
- aucune hypothèse économique inachevée des Phases 10/11 réutilisée ;
- modèles de coûts, latence et fills `CALIBRATED`, avec preuves auditées ;
- frais issus d'un artefact public versionné et hashé, avec intervalles d'effet et
  palier conservateur explicite, jamais d'une lecture privée du compte ;
- cible de 6 à 8 semaines de fonctionnement forward continu, minimum dur 42 jours ;
- 30 à 50 cycles complets pour une stratégie lente, davantage pour une rapide,
  seuil préenregistré avant la fenêtre et impossible à abaisser sous 30 ;
- 14 jours consécutifs sans incident critique ;
- résultat net positif sous coûts stressés, sans exclusion des runs perdants ;
- résultat stressé lié au head économique final, canaux de l'univers figé frais et
  état opérationnel résolu (`FLAT` ou `HEDGED`) ;
- rapport reproductible avec configuration, code, données, journal et preuves
  identifiés par leurs hashes.

Une fixture `SYNTHETIC`, une hypothèse `UNCALIBRATED` ou des tests accélérés ne
comptent ni pour la durée, ni pour les cycles, ni pour les 14 jours. À la date de la
Phase 12, ces observations n'existent pas encore : **Gate D économique `BLOCKED`**.
Une durée supérieure à huit semaines reste recevable. Le gate est calculé depuis
le store vérifié et des preuves persistées, jamais depuis des booléens ou une
projection fournis par l'appelant. Dans ce checkout, le registre runtime stratégie
+ source publique est en outre vide et `paper run` reste
`BLOCKED_PRECONDITIONS`. La conformité technique du moteur n'ouvre donc pas la
Gate E.

## Gate E — testnet

- revue humaine explicite de la Gate D et création d'un composant/version séparé ;
- signatures, annulations et CLOID validés ;
- réponses perdues et événements doublés gérés ;
- dead-man switch ;
- réconciliation après redémarrage ;
- aucune position orpheline.

## Gate F — micro-mainnet

- 100 à 300 USDC maximum au départ ;
- levier 1× maximum ;
- une seule stratégie et une seule paire à la fois ;
- contrôle humain ;
- comparaison fill/slippage/frais prévu vs réel pendant 4 à 8 semaines.
