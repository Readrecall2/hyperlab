# Validation — Phase 04, durcissement du moteur de backtest

Date de validation : 12 août 2026

Checkpoint antérieur aux travaux : `466c6ff`

Branche : `phase-04-backtest-hardening`

## Verdict

La définition de terminé de la Phase 04 est satisfaite pour le **cadre logiciel** et
ses fixtures déterministes. Cette validation ne franchit ni la Gate B (couverture et
qualité des données historiques) ni la Gate C (validation économique). Aucun résultat
synthétique ou non calibré ne constitue une preuve de rentabilité.

## Contrôles globaux

Les contrôles suivants passent sur les sources de cette livraison :

- `ruff check .` ;
- `mypy src/hyperlab` — 60 fichiers source contrôlés ;
- `pytest` — 475 tests réussis.

Le lanceur Python du `.venv` local référence une installation Windows supprimée. Les
commandes `mypy` et `pytest` ont donc été exécutées avec le runtime Python embarqué par
Codex en ajoutant `.venv/Lib/site-packages`. Ce contournement d'environnement ne change
ni les dépendances du projet ni les tests exécutés. Le cache pytest a été désactivé car
le dossier `.pytest_cache` de cette copie de travail n'est pas inscriptible.

## Définition de terminé — vérification explicite

| Critère obligatoire | Statut | Preuve |
|---|---:|---|
| Toutes les observations de décision sont point-in-time, les bougies provisoires sont exclues et l'univers vient du lifecycle historique | Conforme | Sélection as-of sur `received_time`, temps source et finalité; validation fail-closed de toutes les cellules exposées; masque lifecycle; identités futures invisibles aux stratégies; tests PIT, finalité auxiliaire, cross-instrument et delisting. |
| Le plan UTC train/validation/test est sérialisé et hashé avant les essais | Conforme | `split_plan.json` est écrit avant `plan_created`, lui-même antérieur à toute `variant_registered`; le plan canonique contient les trois intervalles UTC et le hash du dataset. |
| La sélection ne reçoit pas le test final, puis une variante figée le révèle une seule fois | Conforme | `SelectionSplitView` ne contient aucune plage finale; sélection liée à un événement validation/WF; événements persistants `variant_selected`, `final_test_frozen`, `final_test_revealed`; un résultat final maximum par plan. |
| Le walk-forward calibre sur le passé et produit des fenêtres OOS non chevauchantes | Conforme | Le callback de fit ne reçoit que chaque train; embargo et ordre chronologique validés; `step >= validation_window`; concaténation OOS refuse doublons et désordre. |
| Chaque variante, perte et erreur est inscrite avant le résultat dans un registre append-only vérifiable | Conforme | Plan puis variantes préenregistrées, y compris les stress avant révélation; succès, pertes, erreurs et interruptions conservés; JSONL chaîné, verrou interprocessus et sidecar head atomique détectant corruption et troncature terminale. |
| Fills, non-fills, partials, délais de jambes et IOC sont simulés sans route réseau ou capacité d'ordre réel | Conforme | États explicites dans `fills.csv`; maker probabiliste seedé, capacité/profondeur, délais et IOC risk-reducing; test AST sans imports réseau/venue dans `hyperlab.backtest`; interdiction globale de `hyperliquid.exchange.Exchange`. |
| Le ledger et toutes ses ventilations réconcilient exactement la courbe de capital | Conforme | Identité prix/funding/basis/spread/frais/slippage/hedge contrôlée à chaque run; agrégations actif, mois UTC, régime et taille réconciliées; stress de suppression des meilleurs trades recalculé de manière séquentielle et autofinancée. |
| Seeds, hashes, statuts de calibration, stress et intervalles bootstrap sont présents dans les artefacts | Conforme | `validation_oos.json`, registre, rapports et `run_manifest.json` portent seeds, hashes code/data/split/variant, preuves et statuts de calibration, scénarios préengagés, paramètres et IC; le manifeste vérifie aussi registre et sidecar. |
| `ruff check .`, `mypy src/hyperlab` et `pytest` passent | Conforme | Résultats globaux ci-dessus. |
| Les limites de données empêchent toute prétention de calibration ou validation économique non démontrée | Conforme | Defaults `SYNTHETIC`/`UNCALIBRATED`; bootstrap absent hors OOS; `CALIBRATED` exige un hash SHA-256 de preuve et une source/méthode non-placeholder pour données, coûts et fills maker; avertissements visibles et Gates B/C fermées. |

## Tests anti-biais et de robustesse

- look-ahead : invariance des préfixes des six stratégies et signal `t` rémunéré au
  plus tôt sur `t → t+1` ;
- survivorship bias : lifecycle as-of, conservation des actifs délistés et masquage
  complet des identités encore futures ;
- timestamps : index/colonnes strictement identiques, UTC obligatoire, jointures
  backward par temps reçu, staleness au temps source et refus des gaps bootstrap ;
- données finales : révisions futures ignorées, `is_final=false` exclu avant choix de
  révision, dernière barre du test final strictement mark-only ;
- sélection après observation : provenance registre obligatoire, résultat forgé
  refusé, variantes de stress préengagées avant le gel/révélation finale ;
- suppression des meilleurs trades : trades économiques clôturés, ties déterministes,
  capital et ventilations recalculés sans refit ;
- coûts ×2, probabilité maker dégradée et latence dégradée : scénarios automatiques,
  enregistrés et exportés ;
- objectif : allowlist métrique/direction sans champ cible; alias imbriqués, camelCase
  et distances vers un rendement souhaité refusés.

## Limites de données constatées

Le lake local audité ne contient que `binance_usdm` sur les 11–12 août 2026 et ne
fournit ni trente jours propres ni une couverture Hyperliquid simultanée avec la venue
de référence. Il ne permet donc pas de calibrer économiquement les frais, la profondeur,
les fills maker ou la latence. Les modèles bar-level ne prouvent pas davantage le market
making ou le lead-lag rapide. La Phase 04 valide l'intégrité du cadre, pas une stratégie.

## Exécution de référence synthétique

Une exécution complète et révélée sur une fixture BTC synthétique est disponible dans
`reports/phase04-validation-synthetic/`. Elle contient 15 événements de registre, cinq
résultats (final et quatre stress), 108 artefacts hashés et les quatre ventilations PnL.
Son manifeste a été revérifié fichier par fichier. Tous les résultats portent
`SYNTHETIC`; coûts et fills maker restent `UNCALIBRATED`. Cette exécution démontre le
workflow et le format des artefacts, jamais une performance économique.

## Reproductibilité

Chaque exécution de recherche écrit un plan, un registre vérifiable, les ledgers des
folds OOS, les paramètres complets, les rapports et un manifeste SHA-256. Le fichier
`MANIFEST_SHA256.txt` couvre les fichiers livrés; les résultats de recherche ignorés par
Git disposent en plus de leur propre `run_manifest.json`.

---

# Validation — Phase 05, cash-and-carry spot/perp

Date d'audit : 12 août 2026

Checkpoint antérieur aux travaux : `5e98ff5`

Branche : `phase-05-cash-and-carry`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (61 fichiers) et
`pytest --basetemp reports/.pytest-phase05-final` (482 tests) passent.

## Verdict Phase 05

Le cadre logiciel Phase 05 est implémenté. La stratégie n'est pas promue : le lake
réel local contient trois observations de funding Binance par actif sur environ
24 heures et aucune série Hyperliquid spot/perp simultanée couvrant les 720 heures
de la Gate B. Il manque donc une donnée indispensable ; aucun résultat économique
n'est fabriqué. Statut attendu : `BLOCKED_INSUFFICIENT_REAL_DATA`.

L'inventaire local observé au moment de l'audit contient 88 lignes de snapshots
SQLite Hyperliquid. Le collecteur de référence annonce 1 074 950 lignes publiques,
mais ce volume est principalement composé de BBO/wire/candles minute et ne remplace
ni 30 jours de funding réalisé, ni une paire spot/perp point-in-time complète.

Le validateur ajoute les features causales funding 8/24/72 h, persistance et tendance,
basis/convergence, liquidité, volatilité, OI et edge net multi-horizon. La simulation
réutilise les deux jambes non atomiques du moteur Phase 04, impose des intentions
maker, conserve l'IOC d'urgence risk-reducing, chiffre le capital spot + marge perp,
la capacité par profondeur et la liquidation pré-déclarée. Le stress d'inversion
multiplie réellement la matrice de funding par `-1`.

Le rapport dédié montre rendement sur capital total, temps investi, funding, basis,
frais, spread, slippage, hedge, fermeture, coût d'opportunité, drawdown et capacité.
La gate refuse toute promotion si les preuves sont incomplètes, si la fermeture
échoue ou si la pire surperformance stressée ne dépasse pas le benchmark passif.

## Audit de la démo Cash & Carry

Avant correction, la démo 1200 h/seed 42 examinait 600 observations candidates,
dont 564 complètes, mais n'émettait aucun signal et n'ouvrait aucune position. Les
échecs non exclusifs étaient : funding 184, persistance positive 201, profondeur
284 et edge net multi-horizon 564 ; basis, volume, OI et volatilité n'éliminaient
aucune observation complète. L'edge était donc la gate décisive : même son meilleur
minimum 8/24/72 h restait négatif sur chaque actif après frais, spread, slippage et
coût d'opportunité.

Le zéro était cohérent avec les gates, mais involontaire pour le contrat de la
fixture synthétique, censée exercer chaque module. La correction n'assouplit aucun
seuil : elle ajoute une fenêtre BTC bornée et visible dans les métadonnées. Pour la
même commande, les diagnostics donnent désormais 600 candidates, 564 complètes,
une candidate éligible, un signal d'entrée, une position ouverte, un signal de
sortie, une position fermée et 12 ordres. Le parcours comprend des non-fills maker,
des IOC, du funding positif, des coûts négatifs, du basis et un hedge transitoire.

Un second défaut a été corrigé : après une clôture partielle, le moteur ne retraitait
pas le reliquat si la cible restait à zéro. La démo conservait alors une poussière
spot jusqu'à la fin. Les clôtures incomplètes sont maintenant réconciliées de façon
explicite et le test de non-régression exige des poids finaux exactement nuls.

Cette démo valide le câblage Cash & Carry de bout en bout, pas la stratégie sur le
marché réel. Le verdict économique reste `BLOCKED_INSUFFICIENT_REAL_DATA`.

---

# Validation — Phase 06, basket de funding Hyperliquid

Date d'audit : 12 août 2026

Checkpoint antérieur aux travaux : `3fd7dae`

Branche : `phase-06-funding-basket`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (62 fichiers) et les
491 tests `pytest` passent. La démo synthétique 1 200 h/seed 42 ouvre trois
positions, produit 207 ordres simulés et exerce funding comme coûts. Le manifeste
de livraison contient 186 entrées et se revérifie sans mismatch après
normalisation canonique CRLF vers LF.

## Verdict Phase 06

Le modèle causal et le validateur logiciel sont implémentés. Aucune promotion
économique n'est possible avec le lake local : il ne contient pas 90 jours de
funding Hyperliquid horaire, une cross-section d'au moins six perps avec profondeur
point-in-time et un univers lifecycle incluant des marchés délistés. Le statut réel
reste `BLOCKED_INSUFFICIENT_REAL_DATA`; aucun rendement réel n'est fabriqué.

La baseline classe les fundings persistants et répartit chaque côté en inverse-vol.
L'optimiseur projette le problème risque/carry/turnover dans le noyau des contraintes
de neutralité dollar, bêta BTC et bêta ETH. La covariance échantillonnale est
shrinkée vers sa diagonale. Une mise à l'échelle commune respecte levier brut et
poids maximum sans casser les neutralités au moment du rebalance.

Les filtres d'âge, volume, profondeur, finalité, disponibilité et tradabilité sont
causaux. Un momentum de squeeze interdit un short et force un rebalance de risque ;
le système ne consulte pas le futur pour anticiper une délisting.

La matrice Phase 06 enregistre ranking et optimisation, attribue séparément funding
et performance relative, puis exécute coûts ×2, fills maker dégradés, latence,
corrélation cassée, squeeze simultané des shorts et suppression des meilleurs
trades. Le leave-one-out recalcule l'optimisation après exclusion de chaque actif.
L'audit bloque explicitement un panel sans marché ancien délisté, protection contre
le biais de survivants.

Les tests dédiés couvrent causalité des features, filtres de données, covariance
symétrique positive semi-définie, trois neutralités, poids maximum, effet de la
pénalité de turnover, filtre de squeeze, transformations réelles des stress,
attribution du PnL, leave-one-out et rapport JSON/HTML reproductible.

Les neutralités bêta sont exactes aux décisions de rebalance et peuvent dériver
entre deux décisions quand les estimations changent. Les chocs de corrélation et de
squeeze sont contrefactuels et visibles comme tels ; ils mesurent une sensibilité,
pas une probabilité de crise. La Phase 06 ne crée aucune route d'ordre réel.

---

# Validation — Phase 07, funding inter-exchanges

Date d'audit : 12 août 2026

Checkpoint antérieur aux travaux : `520c906`

Branche : `phase-07-cross-exchange-funding`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (63 fichiers) et les
500 tests `pytest -p no:cacheprovider` passent. Les neuf tests Phase 07 couvrent
calendriers/formules, causalité, bases mark/oracle, liquidation locale, transferts,
pannes, audit, export CSV et rapport JSON/HTML.

## Verdict Phase 07

Le modèle logiciel et les stress 1 h/6 h/24 h sont implémentés. La validation
économique reste `BLOCKED_INSUFFICIENT_REAL_DATA` : le lake événementiel local ne
contient que Binance USDⓈ-M sur les 11–12 août 2026 et aucun historique
Hyperliquid événementiel synchronisé suffisant. Les 88 snapshots du statut legacy
Hyperliquid ne constituent ni trente jours horaires, ni un panel point-in-time de
marks, oracles et règlements sur les deux venues. Aucun rendement réel n'a été
fabriqué.

Le simulateur garde deux equities, marges initiales, marges de maintenance et
marges libres indépendantes. La variation du mark et le funding sont crédités à la
venue concernée. Hyperliquid utilise l'oracle pour le notional de funding et son
mark pour le risque ; Binance utilise le taux réalisé et le mark associés au
règlement. Les taux sont comparés après normalisation par la durée du calendrier,
à partir des seules observations déjà disponibles.

Les frais, le slippage et les pénalités sont débités par jambe. Un transfert retire
montant et frais de la source, place le principal en transit, puis ne crédite la
destination qu'après le délai et si les rails sont disponibles. Une crise peut
bloquer départ et arrivée. La liquidation locale est évaluée avant la prochaine
décision utilisateur et peut survenir alors que le capital total reste positif.
Après liquidation, toute nouvelle entrée est interdite et l'autre jambe est
débouclée dès que sa venue est disponible.

La matrice de panne ne modifie pas le chemin de prix. Elle bloque les actions
utilisateur et transferts de la venue choisie pendant exactement 1, 6 ou 24 heures,
avec une barre de reprise obligatoire. La gate bloque une liquidation locale ou un
temps non couvert dans n'importe quel scénario.

Le rapport reproductible contient rendement brut/net sur capital total, rendement
économique par venue, drawdown, pire heure, turnover, exposition brute/nette, PnL
mark/funding/coûts, capital immobilisé, pire déficit de marge local, temps non
couvert, frais de rebalancing et liquidations. Les hypothèses de risque, calendriers,
formules, bases de prix, provenance et durées de panne sont inscrites dans le JSON.

## Limites connues

La grille est horaire : elle sous-estime potentiellement une liquidation intrabar.
Les valeurs par défaut de marge, frais et transfert sont `UNCALIBRATED` et ne
franchissent jamais l'audit. Le modèle ne couvre pas encore l'ADL, la faillite de
venue, le haircut du stablecoin, les limites de retrait ou la récupération après
insolvabilité. Ces limites sont détaillées dans
`docs/CROSS_EXCHANGE_FUNDING_PHASE07.md`. Aucun client privé, secret, signer ou
chemin d'ordre réel n'a été ajouté.

---

# Validation — Phase 08, pairs trading robuste

Date de validation : 12 août 2026

Checkpoint antérieur aux travaux : `90a68aa`

Branche : `phase-08-pairs-trading`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (64 fichiers) et les
505 tests `pytest -p no:cacheprovider` passent. Les cinq tests Phase 08 couvrent
l'audit anti-survivorship, l'invisibilité du test final pendant la sélection,
la causalité des z-scores/hedges, les stops/cooldown/sizing borné et les deux
stress obligatoires avec rapport JSON/HTML.

## Verdict Phase 08

Le cadre logiciel Phase 08 est implémenté. La validation économique reste
`BLOCKED_INSUFFICIENT_REAL_DATA` : le lake local ne fournit pas 180 jours horaires
multi-actifs Hyperliquid point-in-time avec lifecycle historique, marchés délistés,
funding réalisé, profondeur et coûts/fills calibrés. Les fixtures synthétiques
exercent les invariants mais ne constituent aucune preuve de rentabilité.

Les identités des paires sont classées sur train seulement après contrôles de
chevauchement, corrélation de rendements, demi-vie et stabilité du hedge ratio.
La validation choisit ensuite, pour chaque paire déjà figée, entre rolling, Kalman
causal et cointégration OLS. Modifier tout le test final ne change aucun choix.

Le z-score à `t` normalise le spread courant avec des moments arrêtés à `t-1`.
Le sizing cible la volatilité passée du spread et reste plafonné par paire et au
niveau portefeuille. Les sorties comprennent retour à la moyenne, stop dur de
spread, time stop, indisponibilité d'une jambe et cooldown. Il n'existe ni
martingale, ni doublement après perte, ni moyenne à la baisse non bornée.

Le moteur Phase 04 conserve l'attribution prix/funding/spread/frais/slippage et le
turnover. La gate retire la meilleure paire selon la validation — jamais selon le
test final — et impose une rupture déterministe de corrélation, marquée
`SYNTHETIC`. Une seule paire, une equity non positive/non finie ou un rendement
stressé sous le seuil préenregistré bloque la promotion.

## Limites connues

Le filtre de cointégration utilise une régression OLS et une demi-vie AR(1), pas une
batterie asymptotique complète dépendante d'une bibliothèque statistique externe.
La rupture de corrélation est un contrefactuel déterministe, pas une estimation de
probabilité. La grille horaire ignore les excursions intrabar. Les paramètres
Kalman, seuils de z-score et horizons restent des hypothèses de recherche à
préenregistrer puis valider sur données réelles. Aucun client privé, secret,
signer, martingale ou chemin d'ordre réel n'a été ajouté.

# Validation — Phase 09, momentum et régimes

Date : 12 août 2026

Checkpoint antérieur aux travaux : `5710381`

Branche : `phase-09-momentum-regime`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (65 fichiers) et les
510 tests `pytest -p no:cacheprovider` passent. Les cinq tests Phase 09 couvrent
l'audit fail-closed des données directionnelles, la causalité des features et
régimes, l'invisibilité du test final pendant la sélection, les stops/corrélation/
cooldowns/caps de risque et le rapport anti-dépendance au bull market.

## Verdict Phase 09

Le cadre logiciel directionnel est implémenté et reste séparé des modules
market-neutral. La validation économique demeure
`BLOCKED_INSUFFICIENT_REAL_DATA` : le lake local ne fournit pas 365 jours horaires
multi-actifs point-in-time avec lifecycle historique, marchés délistés, volume,
OI, funding réalisé, liquidations observées, profondeur et coûts/fills calibrés.
Les fixtures `SYNTHETIC` exercent les invariants mais ne prouvent aucun rendement.

Les variantes time-series multi-horizons, breakout et combinée sont toutes
enregistrées. La sélection ne voit que train/validation et la variante choisie est
gelée avant le test final. Volume et OI confirment l'amplitude ; le funding agit
comme coût/confirmation sans pouvoir retourner seul le signal. Tous les calculs à
`t` ne gagnent qu'à partir de `t → t+1`.

Le profil déployable cible la volatilité, applique un stop suiveur de volatilité,
borne le poids par actif, refuse les nouvelles positions trop corrélées et met le
portefeuille à plat pendant un cooldown après spike de liquidations. Le levier brut
est validé à 1× maximum dans la stratégie, la configuration et le résultat rempli.
Il n'existe ni martingale, ni doublement après perte, ni chemin d'ordre réel.

Le rapport JSON/HTML sépare prix, funding et coûts par régime sur le test final. La
gate exige `trend_up`, `trend_down` et `chaos`, un PnL hors `trend_up` au-dessus du
seuil préenregistré et une concentration maximale des profits bull. Une stratégie
qui ne fonctionne que dans un bull market est explicitement rejetée.

## Limites connues

La classification de régime repose sur moyenne et volatilité bar-level, pas sur un
modèle latent. Le breakout utilise les clôtures et le stop la volatilité réalisée :
sans high/low intrabar, les excursions ATR et gaps internes à l'heure restent
inobservables. La limite de corrélation utilise une fenêtre rolling et ne garantit
pas qu'une corrélation ne change pas après l'entrée. Les seuils, horizons, coûts et
notionnels de liquidations doivent être préenregistrés puis calibrés sur données
réelles avant toute conclusion économique.

---

# Validation — Phase 11, market making adaptatif

Date de validation : 13 août 2026

Checkpoint antérieur aux travaux : `2ae488f`

Branche : `phase-11-market-making`

Contrôles globaux : `ruff check .`, `mypy src/hyperlab` (67 fichiers) et les
546 tests `pytest -p no:cacheprovider` passent avec un code de sortie nul. Le cache
pytest est désactivé parce que `.pytest_cache` n'est pas inscriptible dans le sandbox.
Le lanceur du
`.venv` référence un Python 3.12 supprimé ; les contrôles utilisent donc le runtime
Python 3.12 embarqué par Codex avec `src` et `.venv/Lib/site-packages` dans
`PYTHONPATH`, et un `TEMP` local au workspace. Ce contournement ne modifie ni le
code ni les dépendances testées.

## Verdict Phase 11

Le cadre logiciel Phase 11 est implémenté : replay L2 causal par timestamp de
réception, snapshots/en-têtes atomiques, deltas séquencés, trades groupés par trame,
fair value multi-venue, microprice, imbalance, order flow, spread frais/toxicité,
skew et taille d'inventaire, retrait toxique, queue, cancel/replace, fills partiels,
markouts 100 ms/1 s/5 s, adverse selection, hedge taker optionnel, spikes et pannes.

La validation économique reste `BLOCKED_INSUFFICIENT_REAL_DATA`. Le dossier `data`
de ce checkout ne contient aucun lake Phase 11 réel. En outre, les snapshots
Hyperliquid `l2Book` normalisés n'exposent pas de séquence serveur publique ; ils ne
peuvent donc pas satisfaire la gate de couverture sans preuve supplémentaire. Les
frais, latences, position de queue, toxicité et seuils ne sont pas calibrés. Aucun
rendement réel n'est calculé ou revendiqué.

Le simulateur synthétique historique reste explicitement `TOY`. Le nouveau moteur
reste `EVENT_REPLAY_RESEARCH_ONLY`, même si un hash de calibration est fourni. Il
n'existe aucun client privé, secret, signer, import
`hyperliquid.exchange.Exchange`, ni route permettant d'envoyer, modifier ou annuler
un ordre réel.

## Contrôles de causalité et microstructure

- une quote créée à `t` ne peut pas être remplie par le flux reçu à `t` ;
- toutes les transactions d'une même trame reçue sont appliquées aux seules quotes
  préexistantes avant recalcul de l'order flow et des quotes ;
- modifier un carnet futur ne réécrit ni observations ni fills du préfixe ;
- chaque `l2_book_state` est rapproché de tous les niveaux du `snapshot_id`, avec
  niveaux uniques, contigus et nombres bid/ask exacts ;
- un delta non croissant est refusé et un gap suspend la cotation jusqu'à une
  resynchronisation explicite ;
- les quantités affichées devant la quote sont consommées avant tout fill ; une
  baisse de quantité L2 réduit seulement la file devant nous, une hausse n'ajoute
  pas de priorité devant un ordre déjà posé ;
- un carnet qui traverse une quote active sans trade public ne fabrique pas de
  fill : il bloque l'état comme trade-through non résolu ; avant activation, il
  compte un rejet post-only simulé ;
- un remplacement attend la latence d'annulation et repart derrière la quantité
  affichée, tandis que l'ancienne quote reste exposée avant acknowledgement ;
- une venue configurée mais stale bloque la fair value au lieu d'un fallback
  silencieux ;
- une coupure compte les quotes abandonnées, interdit les fills fantômes et laisse
  le statut `BLOCKED_UNRECONCILED_QUOTES` sans resynchronisation ;
- les markouts séparent spread capturé et déplacement adverse de fair value.

## Audit, rapports et gates

`market-making-audit` valide les manifestes immuables du lake, les en-têtes et
niveaux L2, les trades, deux venues, les timestamps UTC, l'absence d'égalité
inter-frame ambiguë, les séquences cibles, les gaps, les resynchronisations et la
preuve SHA-256 de calibration. `market-making-replay` écrit un readiness report et
s'arrête avant simulation si cet audit échoue.

Le rapport JSON/HTML reproductible publie configuration et manifestes hashés, PnL
net/spread/frais/hedge, markouts et adverse selection, inventaire maximal, taux
maker/taker, fill ratio, cancel-to-fill, fills partiels, tailles, retraits toxiques,
spikes, gaps, pannes et quotes abandonnées. Une étiquette `SYNTHETIC`, `TOY`,
`UNVERIFIED`, `DEFAULT` ou `PLACEHOLDER` ne peut pas être déclarée `CALIBRATED`.

## Limites restantes

Le carnet agrégé ne fournit pas l'identité order-by-order, la liquidité cachée,
les acknowledgements ou rejets privés, ni la véritable position de notre ordre.
Les variations de quantité affichée ne distinguent pas parfaitement annulation et
exécution. La calibration doit être effectuée chronologiquement sur train, figée
sur validation puis évaluée sur un test final non utilisé pour le réglage. Ces
limites et les commandes de reproduction sont détaillées dans
`docs/MARKET_MAKING_PHASE11.md`.
