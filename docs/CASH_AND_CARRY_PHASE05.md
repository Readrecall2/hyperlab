# Phase 05 — cash-and-carry spot/perp

## Verdict actuel

Le validateur logiciel est disponible, mais la stratégie n'est **pas promue**.
Le lake local du 12 août 2026 contient environ 24 heures de référence Binance
USD-M, seulement trois échéances de funding par actif et aucune histoire
Hyperliquid spot/perp simultanée de 30 jours. Les frais et fills restent
`UNCALIBRATED`. La sortie correcte est donc
`BLOCKED_INSUFFICIENT_REAL_DATA`, indépendamment du rendement d'une démo.

## Données exigées

Une exécution économique accepte uniquement un panel UTC horaire, régulier et
point-in-time comportant :

- une identité Hyperliquid spot/perp vérifiée pour chaque actif ;
- prix, paiements de funding horaires réalisés (jamais une estimation courante),
  spread, volume et profondeur exécutable des deux jambes ;
- open interest perp ;
- disponibilité, finalité et univers historique/lifecycle ;
- au moins 720 heures, conformément à la Gate B des stratégies lentes ;
- statuts et hashes de calibration des données, frais et fills maker.

`open_interest_usd.csv` fait désormais partie de l'export panel. Une absence ou un
trou sur le perp ferme la gate au lieu d'être imputé.

Audit seul :

```powershell
.\.venv\Scripts\python.exe -m hyperlab carry-audit `
  --data data\panels\carry-real-v1 `
  --output reports\carry-readiness.json
```

## Signal causal

À chaque heure `t`, le signal utilise uniquement le préfixe terminé à `t` :

- sommes de funding 8 h, 24 h et 72 h ;
- part positive sur 72 h ;
- tendance `moyenne 8 h - moyenne 24 h` ;
- basis perp/spot et réduction de son module sur huit heures ;
- profondeur et volume spot/perp ;
- volatilité annualisée causale sur 24 h ;
- OI perp ;
- edge net projeté à 8 h, 24 h et 72 h.

L'edge soustrait frais aller-retour du compte, spread, slippage conservateur et
rendement passif abandonné. Une paire n'entre que si les trois horizons restent
positifs après coûts et si tous les filtres de liquidité, OI, volatilité et basis
sont satisfaits. Le moteur ne rémunère la cible calculée à `t` qu'à partir de
`t → t+1`.

## Capital et exécution simulée

La taille est exprimée sur le capital total. Par défaut, 50 % du capital peut être
immobilisé et la marge perp réservée vaut 100 % du notionnel :

```text
notionnel par jambe = fraction de capital / (1 + fraction de marge perp)
capital immobilisé = spot + marge perp conservatrice
```

Les deux jambes appartiennent à un `hedge_group` ordonné. Elles tentent un fill
maker indépendant ; timeout, fill partiel, ordre manqué et IOC d'urgence sont des
événements simulés et seedés. L'IOC ne peut que réduire un risque ou combler une
jambe déjà ouverte. Le ledger expose profondeur, participation, capacité par taille,
spread, frais, slippage et PnL de hedge transitoire.

Le test final réserve des barres de liquidation déclarées dans le plan avant la
révélation. Elles mettent les cibles à zéro sans exposer la marque terminale au
signal. Toute exposition encore ouverte ferme la gate.

## Validation synthétique déterministe

La commande `demo --strategy cash_and_carry --hours 1200 --seed 42` contient une
courte fenêtre BTC déclarée dans `synthetic_validation_scenarios`. Elle est conçue
uniquement pour exercer le chemin logiciel : une entrée, deux jambes maker non
atomiques, un hedge IOC, du funding encaissé, les coûts et une sortie entièrement
réconciliée. Les seuils économiques de la stratégie ne sont pas modifiés.

Le tableau CLI publie les nombres de signaux d'entrée, de positions effectivement
ouvertes et d'ordres. Les diagnostics détaillent aussi les échecs et survivants de
chaque gate. Le moteur réessaie une clôture maker/IOC manquée ou partielle tant que
la cible est nulle ; il n'abandonne plus un reliquat d'exécution jusqu'à la fin du
backtest.

Cette fixture est `SYNTHETIC`, son rendement peut être positif ou négatif selon le
PnL de hedge transitoire, et n'est ni une validation économique, ni une calibration,
ni un moyen de franchir les Gates B/C.

## Stress et rapport

La matrice Phase 05 ajoute l'inversion réelle du funding (`× -1`) aux scénarios
coûts ×2, fill maker dégradé, latence et suppression des meilleurs trades. Le
rapport `carry_report.html` et son `carry_summary.json` affichent. Le rendement sur
capital immobilisé utilise le maximum spot + marge observé comme dénominateur :

- rendement sur capital total immobilisé et temps investi ;
- funding, basis, frais, spread, slippage et hedge en USD ;
- coût de fermeture, coût d'opportunité et comparaison passive ;
- drawdown, fill rate, IOC et capacité minimale par taille ;
- statut de fermeture et décision de gate avec toutes ses raisons.

Exécution complète avec test final explicitement révélé :

```powershell
.\.venv\Scripts\python.exe -m hyperlab backtest `
  --data data\panels\carry-real-v1 `
  --strategy cash_and_carry `
  --output reports\phase05-carry-v1 `
  --reveal-final
```

Sans `--reveal-final`, seuls la sélection walk-forward et ses artefacts OOS sont
produits ; le test final et la gate économique restent verrouillés.

## Gate

La décision est fail-closed :

- `BLOCKED_INSUFFICIENT_REAL_DATA` si les 720 heures ne sont pas présentes ;
- `BLOCKED_UNCALIBRATED` si données, coûts ou fills ne sont pas vérifiables ;
- `REJECTED_STRESSED_BENCHMARK_GATE` si la pire surperformance stressée est
  inférieure au passif, si le drawdown dépasse la limite ou si la fermeture échoue ;
- `PROMOTABLE_RESEARCH_ONLY` uniquement lorsque tous les contrôles passent.

Même ce dernier statut n'autorise aucun ordre : HyperLab 0.2.x ne contient ni clé,
ni signataire, ni transport d'ordre.
