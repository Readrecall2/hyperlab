# Phase 07 — arbitrage de funding inter-exchanges

## Verdict

Le simulateur de recherche à deux comptes de marge est implémenté. Il ne partage
jamais implicitement le collatéral entre Hyperliquid et Binance USDⓈ-M et peut donc
liquider une jambe alors que le portefeuille global reste solvable.

La validation économique reste fermée : `BLOCKED_INSUFFICIENT_REAL_DATA`. Au
12 août 2026, le lake événementiel local contient des fichiers Binance des 11 et
12 août, mais aucun historique événementiel Hyperliquid synchronisé permettant de
construire trente jours continus de marks, oracles et règlements de funding sur les
deux venues. Les 88 snapshots du statut legacy Hyperliquid ne remplacent pas ce
panel horaire point-in-time. Aucun rendement réel n'est calculé ou revendiqué.

## Conventions de funding

Les taux d'entrée du simulateur sont des **règlements réalisés**. Une observation
au temps `t` est d'abord appliquée à la position détenue avant `t`, puis seulement
utilisée pour la décision suivante. Une jambe ouverte à `t` ne reçoit donc jamais
le funding de `t`.

### Hyperliquid

- calendrier : chaque heure UTC ;
- signe : un taux positif est payé par le long au short ;
- formule vanilla documentée : `F_8h = P + clamp(I - P, -0,0005, 0,0005)`,
  puis règlement horaire à `F_8h / 8`, plafonné par la venue ;
- paiement : `quantité × oracle × taux` avec signe opposé pour le long ;
- le mark sert au PnL non réalisé, à la marge et à la liquidation.

Ces deux bases ne sont pas confondues. Voir la documentation officielle
[Funding](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding) et
[Robust price indices](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/robust-price-indices).

### Binance USDⓈ-M

- calendrier : règlements explicites du dataset ; la convention standard de la
  démo est 00:00/08:00/16:00 UTC ;
- aucun intervalle de huit heures n'est inventé si `fundingInfo` ou au moins deux
  règlements observés indiquent autre chose ;
- formule de simulation : taux réalisé publié par règlement, appliqué au mark
  associé au prélèvement ;
- le mark Binance et l'index Binance restent des concepts distincts de l'oracle
  Hyperliquid.

L'API officielle expose `fundingRate`, `fundingTime` et le mark associé dans
[Get Funding Rate History](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History).
Les ajustements de plafond, plancher et `fundingIntervalHours` viennent de
[Get Funding Rate Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info).

`FundingCalendar` accepte aussi une liste ordonnée de règlements UTC explicites.
Cela permet de représenter un changement de cadence sans forward-fill silencieux.

## Comptabilité séparée

Pour chaque venue et chaque heure, le moteur conserve :

- equity locale après variation du mark ;
- quantité et notional locaux ;
- marge initiale immobilisée ;
- marge de maintenance ;
- marge libre ;
- buffer de maintenance ;
- PnL mark et funding ;
- frais, slippage et pénalité de liquidation propres à la jambe.

Le capital total vérifie :

```text
capital total = equity venue A + equity venue B + capital en transit
```

Le PnL net vérifie :

```text
net = variation des marks + funding
      - frais - slippage - pénalités de liquidation
      - frais de transfert de collatéral
```

La stratégie compare des taux ramenés à l'heure à partir des seuls règlements déjà
connus. Elle prend la même quantité de l'actif, positive sur le funding bas et
négative sur le funding haut. L'égalité des quantités neutralise le delta de
l'actif ; les notionals locaux peuvent différer parce que les marks diffèrent.

## Marge, transfert et liquidation

Une venue ne peut jamais consommer la marge libre de l'autre sans transfert
explicite. Quand la marge libre locale passe sous le seuil de rebalancing :

1. le moteur calcule le besoin vers le buffer cible ;
2. il vérifie le surplus de la venue source ;
3. il débite immédiatement montant et frais ;
4. le montant reste `in_transit` pendant le délai configuré ;
5. il n'arrive que si destination et rails de transfert sont disponibles.

Pendant une crise, `transfers_available = false` bloque le départ et l'arrivée. Un
transfert déjà en transit reste compté dans le capital total, mais ne protège
aucune marge locale avant son arrivée.

La liquidation est testée contre le buffer de maintenance **de la venue**, avant
toute nouvelle décision utilisateur. Elle ferme seulement la jambe locale avec
ses coûts et sa pénalité. Après une liquidation, le moteur passe en arrêt incident
et tente de déboucler l'autre jambe. Si cette venue est indisponible, l'exposition
reste non couverte jusqu'à sa réouverture. Aucune nouvelle position n'est ouverte
après l'incident.

## Pannes préenregistrées

La matrice impose exactement :

- `outage_1h` ;
- `outage_6h` ;
- `outage_24h`.

Une panne bloque les trades utilisateur et les transferts sur la venue touchée.
Le moteur de risque local reste actif : supposer qu'une interface indisponible
empêche aussi toute liquidation serait optimiste. Les marks et fundings continuent
d'évoluer selon le chemin historique ou synthétique fourni ; la panne ne réécrit
pas artificiellement les prix.

## Rapport reproductible

`write_cross_exchange_report` produit :

- `cross_exchange_funding_summary.json` ;
- `cross_exchange_funding_report.html` ;
- `cross_exchange_funding_timeline.csv` ;
- `cross_exchange_funding_trades.csv` ;
- `cross_exchange_funding_transfers.csv` ;
- `cross_exchange_funding_liquidations.csv`.

Le JSON contient rendement brut et net sur capital total, rendement économique par
venue, drawdown, pire heure, turnover, expositions, capital immobilisé, marge libre,
déficit de maintenance, temps non couvert, coûts de transfert, liquidations et PnL
par composante. L'HTML compare base et indisponibilités 1 h/6 h/24 h.

## Commandes

Démo déterministe, toujours marquée `SYNTHETIC` :

```powershell
.\.venv\Scripts\python.exe -m hyperlab cross-exchange-demo `
  --hours 240 `
  --output reports\cross-exchange-demo
```

Audit fail-closed d'un export Phase 07 :

```powershell
.\.venv\Scripts\python.exe -m hyperlab cross-exchange-audit `
  --data data\cross-exchange-final
```

Simulation de recherche et stress :

```powershell
.\.venv\Scripts\python.exe -m hyperlab cross-exchange-backtest `
  --data data\cross-exchange-final `
  --output reports\cross-exchange-final `
  --failed-venue HL
```

L'export doit contenir `mark_prices.csv`, `oracle_prices.csv`,
`funding_rates.csv`, `cross_venue_manifest.json` et `metadata.json`. Les fichiers
optionnels `venue_available.csv` et `transfers_available.csv` représentent les
incidents observés. Le manifeste fige les calendriers, formules et bases de prix ;
le loader refuse une convention différente au lieu de l'inférer.

Pour franchir l'audit, `metadata.json` porte aussi `venue_risk_rules` pour les deux
venues : fractions de marge initiale/maintenance, frais, slippage, pénalité de
liquidation et hash SHA-256 de leur preuve de calibration. En leur absence, la CLI
emploie uniquement les hypothèses conservatrices `UNCALIBRATED` de la démo et la
gate reste fermée.

## Gate de données

La gate exige au minimum :

- trente jours horaires, réguliers et alignés ;
- marks, oracles/index et règlements réalisés complets ;
- provenance réelle point-in-time et hash de calibration ;
- identité économique vérifiée des deux contrats linéaires ;
- conventions de funding sourcées et hashées ;
- frais, slippage, marges et pénalités calibrés par venue ;
- politique, délai et coût de transfert sourcés ;
- paramètres figés avant la période ;
- période identifiée comme validation, test final ou forward.

Le test final ne sert jamais au réglage. Les scénarios de panne sont fixés dans le
code avant le résultat et ne peuvent pas être remplacés par une sélection a
posteriori.

## Limites

- Le moteur utilise une variation de marge horaire ; une liquidation intrabar peut
  être pire que le résultat observé.
- Les tiers de maintenance et les pénalités changent avec la taille et doivent être
  fournis par un export calibré avant toute conclusion réelle.
- Les frais de transfert peuvent dépendre du réseau, du stablecoin, des limites de
  retrait, des files manuelles et de contrôles de conformité non observables.
- Une panne ne prouve pas la perte des données, l'impossibilité de liquidation ou
  la solvabilité de la venue.
- Le modèle ne représente ni faillite de venue, ni haircut de collatéral, ni ADL,
  ni récupération judiciaire.
- Aucun fill maker n'est supposé : les coûts de la Phase 07 sont taker/slippage
  explicites par jambe.
- Ce module n'importe aucun exécuteur, ne signe rien et ne peut envoyer, modifier
  ou annuler un ordre.
