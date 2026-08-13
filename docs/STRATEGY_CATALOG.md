# Catalogue des stratégies

## Contrat de validation Phase 04

Chaque stratégie déclare tous ses paramètres dans le registre avant exécution. Sa
calibration reçoit uniquement le train; la comparaison se fait sur validation
walk-forward, puis une seule variante figée accède au test final. Les rapports
montrent le benchmark passif, le PnL par composante/actif/mois/régime/taille et les
scénarios coûts ×2, maker dégradé, latence dégradée et meilleurs trades supprimés.

Les stratégies multi-jambes déclarent leurs `hedge_groups` : carry spot/perp,
funding inter-venues et paire statistique. Le moteur mesure ainsi le délai, le PnL
de hedge transitoire et les IOC simulés. Basket, momentum et lead-lag conservent une
attribution de prix directionnelle ou cross-sectionnelle.

Les données et modèles doivent porter `CALIBRATED`, `UNCALIBRATED` ou `SYNTHETIC`.
Seul le premier statut, avec Gates B et C satisfaites, permet une conclusion
économique; aucun statut n'autorise une exécution réelle dans la branche 0.2.x.

## Niveau 1 — cash-and-carry spot/perp

**Position :** long spot + short perp du même actif lorsque le funding positif semble suffisamment persistant.

**Source du PnL :** funding reçu et éventuelle convergence du basis.

**Risques :** frais spot élevés, inversion du funding, basis, jambe non couverte, marge du perp, liquidité du spot.

**Données minimales :** spot/perp, funding horaire, BBO, profondeur, volume, OI,
frais du compte et lifecycle point-in-time. La Phase 05 exige 30 jours, calcule
l'edge net à 8/24/72 h, simule maker puis hedge IOC, réserve une marge perp
conservatrice et refuse la promotion si le pire stress ne bat pas le passif. Voir
[`CASH_AND_CARRY_PHASE05.md`](CASH_AND_CARRY_PHASE05.md).

## Niveau 2 — basket de funding

**Position :** long des perps au funding faible ou négatif ; short des perps au funding élevé.

**Source du PnL :** écart de funding cross-sectionnel.

**Risques :** actifs différents, bêta BTC résiduel, squeeze des shorts, rotation coûteuse.

**Validation Phase 06 :** score de funding persistant, âge et liquidité minimum,
ranking inverse-vol comparé à une optimisation dollar et bêta BTC/ETH neutre,
covariance shrinkée, limite par actif, pénalité de turnover et filtre de squeeze.
Le rapport sépare funding et performance relative, choque corrélations et shorts,
exclut chaque actif et exige un univers lifecycle contenant les marchés délistés.
Voir [`FUNDING_BASKET_PHASE06.md`](FUNDING_BASKET_PHASE06.md).

## Niveau 2 — arbitrage de funding inter-exchanges

**Position :** même actif, long sur la plateforme au funding inférieur et short sur la plateforme au funding supérieur.

**Source du PnL :** différence de funding et convergence des prix.

**Risques :** capital séparé, mark/oracle différents, panne d'une venue, transfert impossible, liquidation locale malgré neutralité globale.

**Validation Phase 07 :** simulateur événementiel à deux comptes de marge,
calendriers et bases de funding propres à chaque venue, frais/slippage par jambe,
transferts différés ou bloqués, liquidation locale, rebalancing de collatéral et
pannes préenregistrées 1 h/6 h/24 h. Le rapport expose rendement total et par
venue, déficit de marge, temps non couvert et coûts de transfert. Voir
[`CROSS_EXCHANGE_FUNDING_PHASE07.md`](CROSS_EXCHANGE_FUNDING_PHASE07.md).

## Niveau 3 — pairs trading

**Position :** long d'un actif et short d'un autre lorsque leur écart statistique s'éloigne de son régime estimé.

**Source du PnL :** retour à la moyenne du spread.

**Risques :** relation structurelle cassée, estimation instable du hedge ratio, stops fréquents.

**Règle :** jamais de martingale ; taille fixée par le risque, stop de spread et délai maximal de détention.

## Niveau 3 — momentum / régime

**Position :** directionnelle, suivant la tendance lorsque le signal dépasse le bruit et que le régime est compatible.

**Source du PnL :** persistance des mouvements.

**Risques :** faux breakouts, whipsaw, gaps, funding payé, levier directionnel.

**Validation Phase 09 :** comparaison sur validation du momentum time-series
multi-horizons, du breakout et de leur combinaison, avec confirmation volume/OI,
funding traité comme coût, volatilité réalisée et régimes causaux. Le risque impose
volatilité cible, stop de volatilité, caps total/par actif de 1× maximum, limite de
corrélation et cooldown après spike de liquidations observé. Le rapport ventile le
PnL par régime et rejette une performance concentrée uniquement en `trend_up`. Voir
[`MOMENTUM_REGIME_PHASE09.md`](MOMENTUM_REGIME_PHASE09.md).

## Niveau 4 — lead-lag multi-exchange

**Position :** utilise une variation sur une venue de référence pour prédire une réaction très courte d'Hyperliquid.

**Source du PnL :** retard transitoire de prix.

**Risques :** avantage entièrement absorbé par latence/frais, timestamps non synchronisés, sélection adverse.

**Exigence :** données sub-seconde, horloge NTP, mesure de latence, replay réaliste. Le baseline horaire inclus ne valide rien en production.

## Niveau 4 — market making adaptatif

**Position :** ordres bid/ask post-only autour d'une valeur de référence, décalés selon l'inventaire et retirés en régime toxique.

**Source du PnL :** spread capturé et éventuels avantages maker.

**Risques :** adverse selection, file d'attente inconnue, inventaire, cancel tardif, panne, événement brutal.

**Implémentation Phase 11 :** replay L2 event-by-event déterministe, fair value
multi-venue, queue agrégée, latences quote/cancel, perte de priorité au replace,
fills partiels, markouts 100 ms/1 s/5 s, retrait toxique, hedge taker optionnel et
pannes fail-closed. Le moteur est `EVENT_REPLAY_RESEARCH_ONLY` ; la démo
synthétique antérieure reste `TOY`.

**Exigence restante :** séquences cibles observables, modèle de queue/latence/frais
calibré sur données réelles, rejets et acknowledgements privés, validation
chronologique hors échantillon. Voir [`MARKET_MAKING_PHASE11.md`](MARKET_MAKING_PHASE11.md).

## Exclusions

HyperLab exclut les martingales, le doublement après perte et les grids non bornées. Leur taux de victoire peut paraître excellent jusqu'à une perte terminale.
