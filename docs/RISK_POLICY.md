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

## Testnet Phase 13

Le service séparé `services/testnet-executor` 0.3.0.dev0 exige un reçu exact
`TESTNET` / `TESTNET_EXECUTION`. Ni Gate B/C/D ni résultat de rentabilité ne
sont requis pour sa préparation technique. Les limites sont toutefois réalistes
afin d'exercer la future architecture d'exécution ; elles ne constituent jamais des
limites Mainnet et ne peuvent pas être converties en autorisation argent réel.

Ces douze valeurs sont les défauts **et** les plafonds compilés conservateurs :

| Champ | Défaut = plafond compilé |
|---|---:|
| notionnel brut maximal | `1000` |
| notionnel maximal par position | `500` |
| notionnel maximal par ordre | `100` |
| quantité maximale par position | `5` |
| quantité maximale par ordre | `1` |
| ordres simultanés | `4` |
| submits par minute | `12` |
| cancels par minute | `24` |
| replaces par minute | `6` |
| âge maximal des données marché | `5 s` |
| âge maximal de la réconciliation | `10 s` |
| intervalle du dead-man switch | `30 s` |

Chaque champ est strictement positif. La configuration peut uniquement l'abaisser ;
elle ne peut dépasser aucun plafond compilé. Toute baisse change le hash de
configuration et son reçu. Une hausse exige une modification du build et une revue
humaine, jamais un override de stratégie/CLI/runtime. Les décimaux exacts sont
refusés si leur coefficient dépasse 64 chiffres, si `|exponent| > 64` ou si
`|adjusted| > 64` ; floats, non-finis et exposants compacts extrêmes échouent avant
formatage/allocation.

Le contrôle projette le pire cas de toutes les positions et réservations des ordres
`REQUESTED`, `SUBMITTED`, `ACKNOWLEDGED`, `OPEN`, `PARTIALLY_FILLED`,
`CANCEL_REQUESTED` et `UNKNOWN`. Prix/taille invalides, données ou
réconciliation stale, débit dépassé, état protecteur, limite absente ou état venue
inconnu entraînent un rejet fail-closed. `reduce_only` ne peut qu'abaisser
l'exposition absolue. Une absence distante ne libère jamais la réservation d'un
submit/replace potentiellement envoyé ; sans preuve venue autoritaire, il reste
ambigu et le runtime passe en `MANUAL_REVIEW`.

Les contraintes live de `meta` sont revérifiées avant `submit` et `replace`. Elles
ne sont pas une dépendance des sorties protectrices : `cancel` utilise le mapping
d'actif figé pour le run avec son CLOID, et `scheduleCancel` reste indépendant de
`meta`.

Les limites de débit ne sont pas locales à une base : leur ledger est durable et à
portée compte dans `%ProgramData%\HyperLab\TestnetExecutor\control-v1`. Le même
registre porte le send gate et le kill latch account-global, plus le watermark de
nonce API-wallet-global. Changer de base, de run ou de processus ne réinitialise
aucun de ces contrôles.

Les identités `SUBMIT/CANCEL/REPLACE` sont des tombstones account-global permanents,
sans éviction ni reset online, avec une capacité compilée de `100000`. Au débit
agrégé maximal de `42/min`, cela correspond à environ `39,7 h` d'actions ordinaires.
Le run doit s'arrêter largement avant la capacité pour revue humaine ; le store
expose `(used, capacity)` et chaque audit d'action lie ces compteurs. Le
`SCHEDULE_CANCEL` protecteur est exempt et reste disponible sans consommer la
capacité.

Au démarrage et après reconnexion/restart, la réconciliation est exchange-first.
Une différence non résolue place le runtime en `MANUAL_REVIEW`. `PAUSED` interdit
les nouvelles entrées ; `KILLED` est durable et exige une action humaine explicite.
`KILLED` est un latch à portée compte, partagé entre bases et sans reset online. Le
dead-man switch est renouvelé sur la venue, mais reste une protection best-effort :
un kill sans bundle d'autorisation, délégué au runtime propriétaire ou non confirmé
sort avec le code `3` tout en gardant le latch. `DEADMAN_ARMED` ne prouve pas que
chaque ordre est déjà annulé et ne ferme aucune position. Une annulation non
confirmée reste ouverte/ambiguë jusqu'à réconciliation. Les limites sont intégrées
au hash de configuration et au reçu ; ni configuration, stratégie ni runtime ne
peut dépasser les plafonds compilés.

Les endpoints et credentials sont strictement Testnet, sans fallback Mainnet. Le
compte doit avoir le rôle `user` et exactement une API wallet active configurée ;
vault, sous-compte ou second writer actif bloquent le preflight. Un
reçu Paper ne peut pas être réutilisé et un reçu Testnet ne peut autoriser ni
micro-mainnet ni Mainnet. Le workflow live et Gate E ne sont pas encore déclarés
validés. Voir [`TESTNET_EXECUTOR_PHASE13.md`](TESTNET_EXECUTOR_PHASE13.md).

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
read-only. Il n'embarque ni runtime paper, ni exécuteur, ni secret. Les runtimes
Paper et Testnet s'exécutent dans des services locaux séparés ; toute phase avec
argent réel exigera une machine et une version dédiées après revue humaine.
