# Phase 08 — pairs trading robuste

## Statut

La Phase 08 fournit un protocole de recherche **read-only**. Elle ne crée aucune
route d'ordre et ne revendique aucun rendement réel. La gate économique reste
fermée tant que le dépôt ne contient pas au moins 180 jours horaires d'un univers
historique point-in-time, avec marchés délistés, funding réalisé, profondeur et
coûts/fills calibrés.

## Protocole causal

Le découpage par défaut est chronologique : 60 % train, 20 % validation et 20 %
test final. Les identités des paires sont classées exclusivement sur train. Le
test final n'entre ni dans les corrélations, ni dans les hedge ratios, ni dans les
tests de stabilité. Pour chaque paire retenue, la validation choisit une méthode
parmi :

- hedge ratio rolling ;
- filtre de Kalman causal ;
- coefficient de cointégration OLS gelé sur train.

Le choix de méthode et tous les paramètres sont gelés avant le début du test
final. Réécrire toutes les observations postérieures à la validation ne change
donc pas la sélection.

## Filtres de stabilité

Une paire doit avoir un chevauchement historique suffisant, une corrélation de
rendements minimale, un hedge ratio positif, une demi-vie finie sous le seuil et
une stabilité acceptable du hedge ratio entre les deux moitiés du train. Chaque
rejet et sa raison sont conservés dans le rapport JSON ; les variantes perdantes
ne sont pas masquées.

Le z-score à `t` utilise le spread observé à `t`, mais sa moyenne, son écart-type
et sa volatilité n'utilisent que les observations jusqu'à `t-1`. Une décision à
`t` ne gagne que sur `t → t+1` via le moteur de backtest commun.

## Risque et coûts

Le sizing cible une volatilité du spread, avec un plafond de gross par paire et
un budget partagé entre les paires. Il ne dépend jamais d'une perte passée. Les
sorties comprennent :

- retour du z-score vers la zone de sortie ;
- stop dur de spread ;
- time stop ;
- fermeture si une jambe devient non tradable ;
- cooldown borné avant toute nouvelle entrée.

Il n'existe ni martingale, ni doublement après perte, ni moyenne à la baisse non
bornée. Le moteur existant attribue séparément performance de prix, funding,
spread, frais et slippage ; le turnover est rapporté.

## Gate Phase 08

Après gel de la sélection, trois chemins sont évalués :

1. test final de base ;
2. retrait de la meilleure paire, identifiée par son score de validation et non
   par sa performance sur le test final ;
3. rupture déterministe et explicitement `SYNTHETIC` des corrélations des paires
   sélectionnées.

La gate exige plusieurs paires, un capital fini et positif, ainsi qu'un rendement
au-dessus du seuil préenregistré dans les deux stress. Un échec produit
`REJECTED_ROBUSTNESS_GATE`. Même en cas de réussite statistique, des données ou
modèles d'exécution non calibrés bloquent la promotion.

## Commandes

```powershell
.\.venv\Scripts\python.exe -m hyperlab pairs-audit `
  --data data\exports\panel `
  --output reports\pairs-readiness.json

.\.venv\Scripts\python.exe -m hyperlab pairs-backtest `
  --data data\exports\panel `
  --output reports\pairs
```

Les artefacts reproductibles sont `pairs_trading_summary.json` et
`pairs_trading_report.html`. Les données indispensables ne doivent pas être
fabriquées pour faire passer la gate.
