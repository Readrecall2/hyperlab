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

- 6 à 8 semaines minimum ;
- 30 à 50 cycles complets pour une stratégie lente, davantage pour une rapide ;
- 14 jours sans incident critique ;
- redémarrages, déconnexions et fills partiels testés ;
- paramètres figés avant la période de validation.

## Gate E — testnet

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
