# Catalogue des stratégies

## Niveau 1 — cash-and-carry spot/perp

**Position :** long spot + short perp du même actif lorsque le funding positif semble suffisamment persistant.

**Source du PnL :** funding reçu et éventuelle convergence du basis.

**Risques :** frais spot élevés, inversion du funding, basis, jambe non couverte, marge du perp, liquidité du spot.

**Données minimales :** spot/perp, funding horaire, BBO, profondeur, volume, frais du compte.

## Niveau 2 — basket de funding

**Position :** long des perps au funding faible ou négatif ; short des perps au funding élevé.

**Source du PnL :** écart de funding cross-sectionnel.

**Risques :** actifs différents, bêta BTC résiduel, squeeze des shorts, rotation coûteuse.

**Améliorations à coder :** neutralité dollar et bêta, inverse-vol, covariance shrinkée, pénalité de turnover.

## Niveau 2 — arbitrage de funding inter-exchanges

**Position :** même actif, long sur la plateforme au funding inférieur et short sur la plateforme au funding supérieur.

**Source du PnL :** différence de funding et convergence des prix.

**Risques :** capital séparé, mark/oracle différents, panne d'une venue, transfert impossible, liquidation locale malgré neutralité globale.

## Niveau 3 — pairs trading

**Position :** long d'un actif et short d'un autre lorsque leur écart statistique s'éloigne de son régime estimé.

**Source du PnL :** retour à la moyenne du spread.

**Risques :** relation structurelle cassée, estimation instable du hedge ratio, stops fréquents.

**Règle :** jamais de martingale ; taille fixée par le risque, stop de spread et délai maximal de détention.

## Niveau 3 — momentum / régime

**Position :** directionnelle, suivant la tendance lorsque le signal dépasse le bruit et que le régime est compatible.

**Source du PnL :** persistance des mouvements.

**Risques :** faux breakouts, whipsaw, gaps, funding payé, levier directionnel.

## Niveau 4 — lead-lag multi-exchange

**Position :** utilise une variation sur une venue de référence pour prédire une réaction très courte d'Hyperliquid.

**Source du PnL :** retard transitoire de prix.

**Risques :** avantage entièrement absorbé par latence/frais, timestamps non synchronisés, sélection adverse.

**Exigence :** données sub-seconde, horloge NTP, mesure de latence, replay réaliste. Le baseline horaire inclus ne valide rien en production.

## Niveau 4 — market making adaptatif

**Position :** ordres bid/ask post-only autour d'une valeur de référence, décalés selon l'inventaire et retirés en régime toxique.

**Source du PnL :** spread capturé et éventuels avantages maker.

**Risques :** adverse selection, file d'attente inconnue, inventaire, cancel tardif, panne, événement brutal.

**Exigence :** replay L2 event-by-event, modèle de queue, délai réel, rejets, fills partiels et cancel acknowledgements.

## Exclusions

HyperLab exclut les martingales, le doublement après perte et les grids non bornées. Leur taux de victoire peut paraître excellent jusqu'à une perte terminale.
