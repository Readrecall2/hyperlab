# Phase 04 — durcissement du moteur de backtest

## Objectif

Transformer le moteur bar-level de démonstration en cadre de recherche crédible.

## Livrables

- walk-forward chronologique ;
- train/validation/test final verrouillé ;
- registre de toutes les variantes et paramètres ;
- coûts par venue/instrument ;
- modèle de slippage dépendant de la taille/profondeur ;
- fills maker probabilistes calibrables et scénario « non rempli » ;
- délai entre deux jambes ;
- sorties IOC d'urgence ;
- PnL détaillé prix, funding, basis, spread, frais et hedge ;
- benchmark passif ;
- bootstrap par blocs et intervalles d'incertitude ;
- rapports par actif, mois, régime et taille.

## Tests anti-biais

- look-ahead ;
- survivorship bias ;
- alignement des timestamps ;
- données finales de bougie ;
- sélection après observation ;
- suppression des meilleurs trades ;
- coûts ×2 et latence dégradée.

Aucun rendement cible ne doit apparaître dans la fonction d'optimisation.

## Définition de terminé

- toutes les observations de décision sont point-in-time (`received_time <= t`),
  les bougies provisoires sont exclues et l'univers vient du lifecycle historique ;
- le plan UTC train/validation/test est sérialisé et hashé avant les essais ;
- la sélection ne reçoit pas le test final, puis une variante figée le révèle une
  seule fois ;
- le walk-forward calibre sur le passé et produit des fenêtres OOS non chevauchantes ;
- chaque variante, perte et erreur est inscrite avant le résultat dans un registre
  append-only vérifiable ;
- les fills, non-fills, partials, délais de jambes et IOC sont simulés sans aucune
  route réseau ou capacité d'ordre réel ;
- le ledger et toutes ses ventilations réconcilient exactement la courbe de capital ;
- les seeds, hashes, statuts de calibration, stress et intervalles bootstrap sont
  présents dans les artefacts ;
- `ruff check .`, `mypy src/hyperlab` et `pytest` passent ;
- les limites de données empêchent explicitement toute prétention de calibration ou
  de validation économique non démontrée.
