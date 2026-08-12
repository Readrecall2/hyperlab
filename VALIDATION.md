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
