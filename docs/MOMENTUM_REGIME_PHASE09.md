# Phase 09 — momentum directionnel et régimes

## Statut

La Phase 09 fournit un validateur de recherche **directionnel**, distinct des
modules market-neutral. Elle ne crée aucune route d'ordre et n'autorise jamais un
levier déployable supérieur à 1×. La gate économique reste fermée sans historique
horaire point-in-time couvrant plusieurs régimes, univers lifecycle avec marchés
délistés, volume, OI, funding réalisé, liquidations, profondeur et modèle
d'exécution calibré.

## Comparaison causale des signaux

Le protocole chronologique conserve 60 % train, 20 % validation et 20 % test final.
Trois variantes préenregistrées sont toutes conservées dans le rapport :

- momentum time-series moyen sur plusieurs horizons ;
- breakout du canal observé avant la barre courante ;
- combinaison momentum et breakout.

La validation choisit une seule variante, puis la gèle avant le test final. Une
réécriture de ce test final ne peut changer ni les scores de validation ni le choix.
Le volume et la variation d'OI confirment ou réduisent l'amplitude sans retourner
le signal. Le funding est traité comme coût/confirmation : un funding positif
pénalise un long et favorise un short, mais ne peut pas inverser seul la direction.
La volatilité réalisée sert au score et au sizing.

## Régimes

Le régime de `t` n'utilise que les rendements terminés avant `t`. La moyenne et la
volatilité récentes sont comparées à une volatilité de référence passée pour
classifier `calm`, `trend_up`, `trend_down`, `chaos` ou `neutral`. Le portefeuille
est plat en `chaos`; en tendance, il ne garde que le sens compatible. Les labels,
leur méthode et leur hash sont enregistrés avec le run.

## Risque directionnel

Le profil déployable initial impose simultanément :

- volatilité cible annualisée convertie en budget horaire ;
- stop suiveur basé sur un multiple de volatilité réalisée, puis cooldown ;
- poids maximal par actif et exposition brute maximale de 1× ;
- sélection gloutonne refusant une nouvelle position trop corrélée aux positions
  déjà retenues ;
- mise à plat au spike de liquidations observé, puis délai obligatoire ;
- aucune martingale, aucun doublement après perte et aucun levier implicite.

Le notionnel de liquidations est une donnée explicite `liquidation_usd`. Un proxy de
volume n'est pas fabriqué lorsqu'elle manque : l'audit échoue fermé.

## Rapport et gate anti-bull-market

`momentum_regime_summary.json` et `momentum_regime_report.html` présentent les
variantes gagnantes et perdantes, rendement net, drawdown, turnover, funding,
coûts, événements de stop/cooldown, expositions et PnL par régime. La gate exige
une couverture de `trend_up`, `trend_down` et `chaos`, un PnL hors `trend_up`
au-dessus du seuil préenregistré et une fraction maximale des profits concentrée
dans `trend_up`. Une performance provenant uniquement d'un bull market est donc
explicitement rejetée.

## Commandes

```powershell
.\.venv\Scripts\python.exe -m hyperlab momentum-audit `
  --data data\exports\panel `
  --output reports\momentum-readiness.json

.\.venv\Scripts\python.exe -m hyperlab momentum-backtest `
  --data data\exports\panel `
  --output reports\momentum
```

Les données synthétiques servent uniquement à tester le câblage. Elles ne peuvent
produire qu'un statut bloqué et ne constituent aucune preuve de rentabilité.
