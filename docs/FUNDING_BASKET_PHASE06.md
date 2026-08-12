# Phase 06 — basket de funding Hyperliquid

## Statut

Le modèle et son validateur sont implémentés en recherche read-only. Il n'ajoute
aucun client d'exécution, ordre, clé ou signature. Le lake réel local ne satisfait
pas encore le contrat de données Phase 06 : la conclusion économique reste
`BLOCKED_INSUFFICIENT_REAL_DATA`.

## Décision causale

À l'heure `t`, le modèle n'utilise que les observations disponibles et finales à
`t`. Une cible décidée à `t` ne gagne que sur `t → t+1` dans le moteur.

Pour chaque perp Hyperliquid :

1. le funding réalisé est moyenné sur une fenêtre passée ;
2. la moyenne est multipliée par une mesure de persistance de signe ;
3. volume et profondeur médians imposent la liquidité minimale ;
4. l'âge est le nombre d'heures observables depuis la première observation ;
5. volatilité et bêtas BTC/ETH sont estimés uniquement sur les rendements passés ;
6. un momentum supérieur au seuil interdit immédiatement de conserver le perp en
   short et déclenche un rebalance de risque.

Le ranking simple prend les fundings les plus faibles en long et les plus élevés
en short, avec poids inverse-vol séparés par côté et neutralité dollar.

## Optimisation contrainte

L'optimiseur utilise le score de carry inverse-vol et une covariance
cross-sectionnelle shrinkée vers sa diagonale :

```text
Σ_shrunk = (1 - λ) Σ_sample + λ diag(Σ_sample)
```

Les poids sont calculés dans le noyau des trois contraintes :

```text
Σ w_i = 0
Σ w_i β_i,BTC = 0
Σ w_i β_i,ETH = 0
```

L'objectif pénalise simultanément la variance prévue et l'écart aux poids
précédents. Une mise à l'échelle commune impose le levier brut et la limite par
actif sans casser les neutralités. Les neutralités sont exactes aux instants de
rebalance ; elles peuvent dériver entre deux décisions lorsque les bêtas estimés
évoluent.

Un actif sous filtre de squeeze est retiré de l'univers optimisable. Cette règle
conservatrice l'empêche aussi d'être utilisé comme hedge long pendant le filtre.
Elle évite qu'une neutralité mathématique recrée indirectement un short interdit.

## Validation obligatoire

Le rapport `funding_basket_report.html` et son JSON réconciliable contiennent :

- ranking inverse-vol contre optimisation contrainte ;
- PnL de funding séparé de la performance relative des prix ;
- coûts, turnover, expositions brute/net et drawdown ;
- choc déterministe de corrélation cassée ;
- hausse simultanée de 20 % des perps détenus short ;
- coûts ×2, fills maker dégradés, latence et suppression des meilleurs trades ;
- exclusion de chaque actif, un par un ;
- liste des marchés présents historiquement mais délistés en fin de panel.

Les stress de prix réutilisent les poids déjà décidés. Ils ne recalculent ni le
ranking ni l'optimisation après avoir vu le choc.

## Contrat de données et gate

Par défaut, l'audit exige :

- grille horaire UTC régulière ;
- 90 jours d'historique ;
- au moins six perps, dont BTC et ETH comme facteurs ;
- funding horaire réalisé, volume et profondeur ;
- disponibilité, finalité et tradabilité point-in-time ;
- provenance et hash de calibration ;
- source et hash de l'univers lifecycle ;
- liste explicite `delisted_assets`, recoupée avec la tradabilité historique ;
- au moins un marché ancien conservé après sa délisting.

L'absence d'un marché délisté bloque la validation au lieu de mesurer uniquement
les survivants actuels. Inclure un actif délisté ne signifie pas supposer une
sortie exécutable après fermeture : les positions doivent être liquidées pendant
une fenêtre encore tradable, selon les informations disponibles à ce moment.

## Exécution reproductible

```powershell
.\.venv\Scripts\python.exe -m hyperlab funding-basket-audit `
  --data data\funding-basket-panel `
  --output reports\funding-basket-readiness.json

.\.venv\Scripts\python.exe -m hyperlab backtest `
  --data data\funding-basket-panel `
  --strategy funding_basket `
  --output reports\phase06-run
```

Le test final reste verrouillé. `--reveal-final` n'est utilisé qu'après gel de la
variante. Les stress Phase 06 sont préenregistrés dans le registre avant cette
révélation. Le rapport dédié est produit seulement après une révélation explicite.

## Limites

- Ce basket n'est pas un arbitrage : les actifs longs et shorts peuvent diverger.
- Les bêtas et la covariance sont des estimations instables en rupture de régime.
- Le filtre de squeeze réduit le risque, sans garantir une sortie disponible.
- Le modèle bar-level ne capture pas liquidation intrabar, impact de liquidation
  collectif ni tous les effets de marge portefeuille.
- Les paramètres de coûts et fills restent `UNCALIBRATED` tant qu'aucune preuve
  réelle versionnée ne les accompagne.
- Un stress contrefactuel vérifie une sensibilité ; il ne prédit pas la fréquence
  ni l'amplitude d'une crise réelle.

Aucun résultat synthétique ou contrefactuel ne constitue une preuve de rendement
ni une autorisation de paper/live trading.
