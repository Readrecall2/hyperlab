# Politique de risque

## Recherche

Chaque stratégie possède un profil de limites séparé. Le moteur limite le poids par instrument, l'exposition brute et l'exposition nette. Les limites ne servent pas à brider le rendement ; elles définissent la stratégie réellement déployable.

## Paper Phase 12

- machine à états persistante ;
- identifiants d'ordres déterministes ;
- traitement idempotent ;
- replay du journal et réconciliation ledger-first au redémarrage avant toute
  nouvelle décision ;
- données périmées = aucune entrée ;
- jambe non couverte = hedge ou débouclage **simulé**, avec reliquat explicite ;
- `REDUCE_ONLY`, `PAUSED`, `MANUAL_REVIEW` et `EMERGENCY_FLATTEN` ;
- watchdog paper persisté ;
- limites de perte quotidienne et drawdown.

Le paper engine applique les limites avant l'ack simulé en calculant l'exposition
projetée après fill. Le profil figé couvre le notionnel par ordre et par instrument,
les expositions brute et nette, la perte journalière, le drawdown, la fraîcheur des
données et la durée maximale d'une jambe non couverte. Les ordres actifs sont
réservés dans le pire cas avant l'acceptation suivante. Une taille invalide, un prix
absent, une donnée stale, un gap non réconcilié ou une limite inconnue entraîne un
rejet, jamais une valeur par défaut permissive.

Les transitions protectrices ne contournent pas le simulateur. `REDUCE_ONLY` ne
peut qu'abaisser l'exposition absolue ; `PAUSED` interdit les nouvelles entrées ;
`MANUAL_REVIEW` bloque la progression automatique ; `EMERGENCY_FLATTEN` tente une
réduction simulée qui peut rester partielle ou non remplie. Toute décision, y
compris un rejet de risque, est persistée et reliée au ledger.

Le statut économique de la Phase 12 reste `BLOCKED` jusqu'à satisfaction de la
Gate D. Les limites ne sont jamais assouplies pour rendre un résultat positif et
les runs `SYNTHETIC`/`UNCALIBRATED` ne comptent pas dans la fenêtre de validation.

## Testnet futur

- composant et version séparés après revue humaine explicite ;
- réconciliation exchange-first au redémarrage ;
- dead-man switch renouvelé sur la venue ;
- aucune réutilisation implicite d'une autorisation paper ;
- mêmes limites codées, avec comportement fail-closed en cas d'état exchange
  inconnu.

## Mainnet futur

- sous-compte réservé au bot ;
- API wallet dédiée et révocable ;
- aucune seed principale sur le serveur ;
- montant initial insignifiant ;
- aucune augmentation automatique ;
- confirmation humaine des premiers cycles ;
- clé accessible uniquement au conteneur exécuteur, jamais au dashboard/collecteur ;
- retrait manuel depuis le wallet principal.

## Umbrel

Le paquet Umbrel actuel reste limité à la collecte publique et au dashboard
read-only. Il n'embarque ni runtime paper, ni exécuteur, ni secret. Le runtime paper
local s'exécute dans un service séparé ; toute phase d'exécution future exigera une
machine et une version dédiées après revue humaine.
