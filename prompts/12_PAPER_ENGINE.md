# Phase 12 — paper trading live

## Objectif

Faire tourner les stratégies figées sur données publiques live avec ordres simulés
réalistes, sans ajouter de capacité de trading réel.

## Frontière absolue

- paper-only : aucune clé, wallet, signature, authentification privée ou route
  permettant d'envoyer, modifier ou annuler un ordre ;
- ne jamais importer ou instancier `hyperliquid.exchange.Exchange` ;
- acknowledgements, rejets, cancels, fills et `EMERGENCY_FLATTEN` sont simulés
  localement ;
- le dashboard reste read-only et ne possède aucun endpoint de commande ;
- aucun passage automatique à la Phase 13.

## Machine à états

`FLAT`, `ENTRY_PLANNED`, `LEG_1_PENDING`, `HEDGE_PENDING`, `HEDGED`,
`EXIT_PLANNED`, `EXIT_PENDING`, `PAUSED`, `REDUCE_ONLY`, `MANUAL_REVIEW`,
`EMERGENCY_FLATTEN`.

Définir une table explicite des transitions autorisées et des invariants. Un fill
partiel conserve son reliquat ; une transition de sécurité ne peut jamais augmenter
l'exposition ; une réduction d'urgence simulée peut échouer ou rester partielle.

## Exigences

- état SQLite persistant, journal append-only chaîné par hashes et transactions
  atomiques ;
- événements idempotents, avec collision même-ID/contenu-différent fail-closed ;
- identifiants SHA-256 déterministes sur sérialisation canonique pour run, décision,
  ordre et événement ;
- configuration immuable du run couvrant stratégie, paramètres, risque, seed,
  versions et hashes de données/calibration ;
- aucune modification pendant la fenêtre : toute modification crée un nouveau run ;
- une seule voie stratégie → contrôle de risque → paper engine ; aucun bypass
  silencieux vers fills, positions, cash ou PnL ;
- risque appliqué avant chaque acceptation simulée sur l'exposition projetée ;
- réutiliser les sémantiques durcies Phase 04 : spread, profondeur, slippage,
  non-fills, fills partiels, IOC, timeouts, délais entre jambes, frais et coûts ;
- latence et fill model calibrés, ou statut explicite `UNCALIBRATED`/`SYNTHETIC` ;
- frais issus d'un artefact **public, versionné et hashé**, avec périodes d'effet et
  palier conservateur ; aucune lecture d'un compte ou endpoint privé ;
- traçabilité décision → ordre simulé → ack/reject → partial/full fill/cancel →
  position/cash/frais/PnL ;
- ledger exactement réconcilié, sans ajustement implicite ; incohérence =
  `MANUAL_REVIEW` et blocage des entrées ;
- replay exact avec mêmes inputs, seed, IDs, événements, fills et hash final ;
- redémarrage fail-closed : vérifier chaîne, rejouer, reconstruire et réconcilier
  avant toute nouvelle décision ;
- snapshot JSON atomique et dashboard read-only ;
- alertes persistées pour corruption, divergence, stale data, dépassement, jambe
  non couverte et échec de réduction ;
- tests de crash aux frontières de transaction, restart avec fill partiel/hedge ou
  cancel en attente, événement dupliqué, troncature et projection obsolète ;
- distinguer visiblement `SYNTHETIC`, `UNCALIBRATED`, conformité technique et
  validation économique ;
- ne pas utiliser les hypothèses économiques inachevées des Phases 10/11 tant que
  leurs prérequis ne sont pas satisfaits ;
- documenter dans `docs/PAPER_ENGINE_PHASE12.md` les invariants, limites et gate.

## Validation technique

Exécuter :

```powershell
ruff check .
mypy src/hyperlab
pytest
```

Faire ensuite une revue critique du diff et une recherche dédiée prouvant l'absence
de secret, signer, client privé et route d'ordre réel. La réussite des tests ne vaut
pas validation économique.

## Gate économique

La Gate D exige cumulativement :

- Gates B/C satisfaites pour la stratégie concernée ;
- 6 à 8 semaines forward avec configuration figée ;
- nombre suffisant de cycles préenregistré (30 à 50 pour une stratégie lente,
  davantage pour une rapide) ;
- 14 jours consécutifs sans incident critique ;
- coûts, latence et fills calibrés avec preuves auditables ;
- crashes, redémarrages, déconnexions, rejets, non-fills et fills partiels exercés ;
- réconciliation et replay exacts ;
- résultat net positif sous coûts stressés, sans masquer les runs perdants ;
- revue humaine explicite avant toute création d'un executor testnet séparé.

Cette durée ne peut pas être satisfaite pendant l'implémentation. Le résultat attendu
à la livraison technique est donc **`BLOCKED` économiquement**, et non « Phase 12
validée ». Les fixtures et démos synthétiques ne comptent pas dans la gate.
