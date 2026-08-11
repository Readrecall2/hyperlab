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
