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

Le statut de promotion économique de la Phase 12 reste `BLOCKED` jusqu'à Gate D.
Cela ne bloque pas un reçu technique exact `PAPER` / `PAPER_RUNTIME` : un
run `SYNTHETIC` ou `UNCALIBRATED` peut exercer le logiciel, reste clairement
non-promouvable et ne compte pas dans la fenêtre de validation. Les limites ne sont
jamais assouplies pour rendre un résultat positif.

## Testnet futur

- composant/version séparés et reçu exact `TESTNET` / `TESTNET_EXECUTION` ;
- ni Gate B/C/D ni résultat de rentabilité requis pour le démarrage technique ;
- endpoint/chain ID Testnet allowlistés sans fallback Mainnet et credentials dédiés ;
- réconciliation exchange-first au redémarrage ;
- dead-man switch renouvelé sur la venue ;
- aucune réutilisation d'un reçu Paper ni conversion vers Mainnet ;
- mêmes limites codées, avec comportement fail-closed en cas d'état exchange
  inconnu.

## Mainnet futur

Le micro-mainnet exige Gates B/C/D/E, revue humaine, configuration signée, signer
isolé, secrets révocables, kill switch/réconciliation et limites codées. `MAINNET`
exige ensuite un reçu distinct, la preuve Gate F et deux revues humaines ;
aucun reçu de classe inférieure ne peut être réutilisé.

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
