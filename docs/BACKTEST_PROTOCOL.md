# Protocole de backtest

## 1. Aucun objectif de rendement

Le moteur doit publier le rendement réellement obtenu : négatif, faible ou très élevé. Aucun plafond ni cible du type « 1 % par mois ».

## 2. Découpage temporel

Exemple initial :

```text
60 % développement / calibration
20 % validation
20 % test final verrouillé
```

Après chaque itération importante, préférer un walk-forward avec paramètres recalibrés uniquement sur le passé disponible.

## 3. Zéro look-ahead

Un signal à `t` est exécuté au plus tôt après `t`. Les données finales d'une bougie ne peuvent pas servir à obtenir son propre rendement. Les listes d'actifs et frais doivent correspondre à la date simulée.

## 4. Coûts

Inclure séparément :

- maker/taker propres à chaque venue ;
- spread ;
- slippage fonction de la taille ;
- funding réellement encaissable ;
- sorties d'urgence ;
- fills partiels ;
- ordres manqués ;
- coût de transfert/rééquilibrage inter-venues ;
- rendement passif abandonné comme benchmark économique.

## 5. Scénarios

Produire au moins :

- idéal/informatif ;
- réaliste ;
- frais et slippage ×2 ;
- fill maker dégradé ;
- latence plus élevée ;
- funding prévu réduit ;
- suppression des meilleurs 1 à 5 % des trades ;
- choc de volatilité et panne simulée.

## 6. Métriques

- rendement total et annualisé ;
- volatilité et Sharpe, avec prudence ;
- max drawdown, Calmar, pire jour et pire heure ;
- exposition brute/net ;
- turnover et temps investi ;
- PnL prix/funding/basis/frais/exécution ;
- distribution des trades et concentration par actif ;
- capacité par taille ;
- comparaison au benchmark passif.

## 7. Market making

Un backtest basé uniquement sur les bougies ne peut pas valider le market making. Il faut rejouer les événements du carnet et modéliser la position dans la file. Le simulateur inclus est une démonstration de plomberie, pas une preuve d'alpha.
