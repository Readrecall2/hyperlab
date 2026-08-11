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

- test final hors échantillon ;
- résultat positif après coûts réalistes ;
- stress ×2 encore acceptable ;
- pas de dépendance à un seul actif ou mois ;
- PnL attribué au mécanisme attendu ;
- avantage économique clair face au rendement passif.

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
