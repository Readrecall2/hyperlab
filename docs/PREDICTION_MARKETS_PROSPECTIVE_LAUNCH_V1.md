# Prediction Markets Prospective Launch V1

Verdict terminal logiciel après admission des reçus forensics réels, fermeture
du bootstrap moniteur et correction du namespace incoming systemd :
`PREDICTION_MARKETS_PROSPECTIVE_LAUNCH_V1_GREEN_SYSTEMD_INCOMING_NAMESPACE_FIXED_COMMITTED_LOCALLY_AWAITING_PUSH`.

Verdict économique : `ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`.

Ce jalon transforme le pack candidat Polymarket + Kalshi en pack de lancement
humain persistant, reprenable, isolé et observable. Aucune commande n'a été
exécutée sur un VPS et aucune campagne H1 n'a été sondée ou modifiée.

## Identités héritées

- base candidate : `3f188b9c28c9fec406b904a9e3307b43f54243e8` ;
- candidate config :
  `aa60c0ff0ef95813d79f56b6ea93a31952061b562905dc9729162f7b16e41964` ;
- campaign manifest logique :
  `c9eb654d077ba1c3cf4cf709c2633077f40fc55d5f9533ce82d498564cd66388` ;
- access bundle :
  `965a42f2169c16201323477c0eb1ba7a8b540b24109c1d9252d5d9fcce55bbe5`.

Le générateur final lie ces identités, le commit de livraison, l'arbre Git
complet, le Git bundle, le wheelhouse Linux et les cinq blocs opérateur dans un
`handoff.json` et un inventaire SHA-256. Les racines sont uniques par tentative.

## Incident `664bef6e` et cause exacte

Le pack historique `pm-20260827t183515z-664bef6e` a passé intégralement le
preflight hôte, créé un manifest authentique, puis B a refusé la readiness du
dashboard avant l'activation de Polymarket ou Kalshi. Le premier refus de
connexion loopback était une course de bind normale. L'exception primaire des
tentatives suivantes était `ModuleNotFoundError: No module named 'numpy'`.

`monitor.sh` utilisait `python3.12 -I`, donc le Python système. Or l'import du
cockpit atteint le runner, le bundle/candidat Prediction Markets puis
`hyperlab.backtest.attribution`, qui importe NumPy. Le bootstrap offline interdit
les system-site-packages et NumPy n'est attesté que dans `.venv`. Le même `try`
capturait cette erreur puis le code appelait `prepared_state_is_stale` hors de ce
bloc; ce nom n'avait jamais été lié. Le `NameError` observé était donc secondaire
et masquait l'erreur d'environnement primaire. Les handoff, manifest, activation
et chemins du pack correspondaient bien au commit `664bef6e`; aucune divergence
de binding n'était la cause primaire.

Le moniteur dérive maintenant le source root canonique depuis son propre chemin,
le lie au handoff SHA-256 et utilise uniquement son Python de venv attesté avec
`-I`. Les helpers statiques sont tous liés et vérifiés appelables avant la phase
de validation runtime. Un échec de runtime/import ou une preuve initiale invalide
produit un JSON borné avec `preflight_error`, `alert=true`,
`operational_failure=true` et une classe fail-closed, jamais un symbole non
défini. La campagne historique
`/mnt/HC_Volume_106716684/hyperlab-prediction-markets/campaigns/pm-20260827t183515z-664bef6e`
et ses racines associées restent préservées; le rollback humain a publié
`PREDICTION_ROLLBACK_DISARMED_RAW_PRESERVED`.

## Services et persistance

Polymarket et Kalshi sont deux unités systemd séparées. Chacune pilote uniquement
un ordonnanceur mono-venue qui appelle la commande canonique
`hyperlab research-data prediction-collect`. Il ne remplace ni l'adaptateur, ni
les envelopes, ni les segments/manifests, ni la récupération raw.

Chaque venue possède un ledger JSONL append-only chaîné par SHA-256. Il conserve
les résultats raw authentifiés, erreurs publiques, créneaux manquants et
interruptions récupérées, sans transformer une métrique absente en zéro. Après
restart, un output existant est
authentifié ou récupéré depuis ses segments publiés; il n'est jamais recollecté.
Les créneaux écoulés sont `MISSING_SLOT_NO_BACKFILL`.

### Reçu authentique mais source publique invalide

Le runtime sépare désormais deux classes qui ne doivent jamais être confondues :

- une incohérence du reçu, du plan, des identités, des hashes, du manifest raw
  ou du ledger reste `INTEGRITY_FAILED`, sort avec le code 4 et ne redémarre pas
  en boucle ;
- un reçu canonique dont tous ces contrôles sont verts, mais dont le terminal est
  `PUBLIC_SOURCE_INVALID`, exige une erreur UTF-8 non vide bornée à 2 048 octets.
  Il est écrit une seule fois dans le ledger avec manifest, root, compteurs et
  provenance, puis le service passe à `WAITING_NEXT_SLOT` sans rejouer
  l'ordinal.

Un tel slot reste explicitement `source_usable=false`,
`economic_eligible=false` et
`CAMPAIGN_BOUND_EXPLICIT_GAP_EXCLUDED_FROM_ECONOMICS`. Il n'entre dans
aucun dataset, évaluation ou holdout économique. Le cockpit et le moniteur le
présentent comme alerte de qualité de donnée propre à la venue, avec
`command_verified=true` si le service est bien celui attendu ; cela ne transforme
jamais le runner en succès économique ni la preuve en donnée exploitable.
Un bundle composé uniquement de tels reçus reste donc
`INSUFFICIENT_PUBLIC_CORPUS`, jamais `OBSERVED_PUBLICLY`.

La régression metadata-only est liée au forensic réel exporté sans segment raw :

- inventaire forensic :
  `4f44b2f151e9ed28c2a6ac10dd719de5a096759b3f0d3cc07b8c0f9a29bdae16` ;
- archive :
  `6e7c094dfb45d901f2f1b77bde8e53958075e9e67349e5cb9f4d125b1c031ea8` ;
- source échouée conservée :
  `6f59caae46e7f473cee9dec00103f4157920f8cb`.

Les deux résultats réels et leurs manifests passent le contrat terminal exact
sans lecture de raw. Les tests de pipeline complet utilisent, séparément, des
segments synthétiques marqués comme tels afin de prouver écriture du ledger,
reprise et absence de rejeu sans présenter ces fixtures comme preuve économique.

### Décision sur les deux parseurs publics

La documentation officielle actuelle Polymarket décrit la réponse compacte CLOB
V2 de `GET /clob-markets/{condition_id}` avec les identités de tokens sous
`t[].t` et les outcomes sous `t[].o`. L'adaptateur accepte uniquement ce schéma
sur ce chemin officiel exact, le lie aux deux paires token/outcome Gamma déjà
authentifiées et refuse les aliases legacy ou les payloads mixtes. Le forensic
borné ne contient aucun segment raw : il ne permet donc pas d'attribuer la
divergence réelle à un schéma précis. Cet alignement documentaire n'est pas
présenté comme la cause prouvée de l'échec ; il durcit la normalisation sans
relâcher la chaîne event → market → outcome/token. Désormais, le corps CLOB
fautif est aussi publié dans l'envelope raw bornée avant que le validateur ne
lève le terminal `PUBLIC_SOURCE_INVALID`, afin qu'un futur forensic conserve la
preuve causale authentifiée.

Pour Kalshi, la documentation actuelle de
`GET /events/{event_ticker}/metadata` expose directement `market_details` et
`settlement_sources`. L'adaptateur lie cette réponse au chemin réellement appelé
et exige que le marché sélectionné apparaisse exactement une fois. Le forensic
ne contient aucun segment raw : il ne permet donc pas d'identifier quelle valeur
temporelle concrète avait déclenché l'erreur négative. Aucune coercition ni
tolérance de timestamp n'a été ajoutée ; toute valeur absente reste absente et
toute valeur présente doit encore être un epoch UTC non négatif et borné.

Le troisième service persistant est un cockpit séparé sur `127.0.0.1:18081`.
Deux unités oneshot `Restart=no` prouvent en plus le namespace de chaque venue
avant le moindre runner. Les cinq unités sont non-root, `ProtectSystem=strict`,
`NoNewPrivileges`, sans capacité, sans secret et avec surfaces d'écriture
limitées aux racines de venue.

## Admission et cohabitation

Le preflight découvre le volume réel au lieu de supposer le volume Hetzner 200
GB. Il exige ext4 rw, fsync, NTP, CPython 3.12 x86_64 avec glibc >= 2.28,
imports offline, wheelhouse multi-tags `manylinux_2_28` + `manylinux_2_17`
hashé, Git et bundle exacts, port 18081 libre, racines/services absents et
connectivité publique officielle bornée. Les parents dédiés `sources/` et
`campaigns/` sont refusés s'ils sont des symlinks ou quittent leur chemin réel,
puis réattestés avec propriétaire/mode avant tout clone ou préparation.

La cohabitation réserve 144 GiB pour H1, 21 GiB pour les 1 344 shards Prediction
Markets et 16 GiB de marge, soit 181 GiB libres. Si cette marge n'est pas prouvée,
le pack refuse et recommande un hôte ou volume distinct sans toucher à H1.

L'essai `73c6d2d2` a observé environ 296,3 GB disponibles pour
`194 347 270 144` octets requis. Cette mesure historique n'est pas réutilisée
comme preuve pour un prochain B. Le
pack réauthentifie après bootstrap l'identité du handoff/source/inventaire et les
hashes des unités, remesure NTP/montage/capacité avant toute mutation systemd,
puis remesure encore NTP/montage/capacité immédiatement avant les collecteurs.
Si H1 ou les preuves historiques ont consommé la marge, le lancement refuse avec
recommandation d'agrandir ou de choisir un autre volume ext4. Les réserves ne
sont jamais réduites et aucune tentative historique n'est supprimée.

Chaque démarrage ou redémarrage systemd d'un runner réauthentifie également le
handoff, l'admission d'installation, l'inventaire transféré, l'inventaire et le
commit source, l'utilisateur/HOME, NTP, les racines canoniques et le même device
ext4 avant de sélectionner un ordinal. Le contrôle de capacité conservateur
lié au ledger reste ensuite exécuté immédiatement avant le créneau. Un échec de
cette admission sort avec le code 4, avant tout enfant de collecte, et
`RestartPreventExitStatus=4` empêche une boucle de redémarrage.

L'admission hôte continue d'exiger la vue interactive du volume ext4 `rw`. Sous
`ProtectSystem=strict`, le target volume admis est exact et read-only. systemd
peut coalescer les `ReadOnlyPaths` imbriqués : les targets effectifs de
`volume_base`/source/campagne sont donc strictement allowlistés entre le volume,
le `volume_base` et le chemin exact concerné. Pour l'incoming, seul le chemin
exact ou un ancêtre canonique de sa chaîne entre `/home` et l'incoming est admis;
`/`, autre home, cousin, descendant arbitraire et symlink restent refusés. Seul
le bind venue reste target exact `rw`. `ReadOnlyPaths` applique cette vue sans
modifier H1. L'admission compare `MAJ:MIN` à `stat(2)`, le fstype et la relation
`SOURCE`/`FSROOT` dérivée du target effectif. Le probe venue fait une création exclusive,
fsync fichier/répertoire, suppression et second fsync, puis le runner répète la
preuve avant tout ordinal. Un autre device,
fstype, bind, symlink ou échec fsync reste un refus fail-closed.

La tentative historique `pm-20260827t234404z-73c6d2d2` a passé les admissions
hôte, installation, capacité et activation guard. Le premier probe namespace
Kalshi a ensuite refusé avant tout service persistant : `_mount_evidence`
reconnaissait correctement un ancêtre de l'incoming coalescé par systemd, mais
`_authenticate_incoming_namespace_target` n'acceptait littéralement que `/home`
ou l'incoming exact. La cleanup a désarmé les unités et aucun collecteur n'a
démarré. Cette racine et toutes ses preuves restent immuables.

Une venue DNS/HTTPS/WSS indisponible reçoit son propre verdict; l'autre venue et
le cockpit restent installables. Kalshi WSS n'est jamais sondé car le contrat
documenté exige une authentification; la collecte Kalshi V1 reste REST publique.
Toute reprise revalide d'abord l'inventaire transféré, le HEAD Git exact et
propre, l'inventaire source, NTP, ext4 `rw`, capacité, racines et imports offline,
puis refait le preflight public borné venue par venue avant réactivation. Une
seconde reprise partielle accepte seulement un collecteur déjà actif dont
l'unité, la commande, le state et le ledger sont authentifiés; un nouvel échec
reste archivé et ne bloque pas l'autre collecteur.

## Démarrage et observation

Le quick-start fixe la campagne à l'heure UTC d'activation après réussite de
l'installation. L'opérateur peut fournir un `HYPERLAB_PM_START_AT_UTC` explicite,
borné à +24 h. Le pack ne contient donc aucune date future artificielle.

Le cockpit expose uniquement GET/HEAD, `mode=readonly` et
`orders_enabled=false`. Il conserve le holdout `SEALED`, refuse les lectures
filesystem non sûres et affiche `NON DISPONIBLE` pour toute métrique absente. Sa
readiness exige le preflight et un reçu d'activation auto-hashé, lié au
`campaign_id`, au SHA du manifest, au commit et à la racine. Le moniteur lie en
plus ce reçu au handoff et authentifie ledger/state avant d'afficher une venue.
Un state `PREPARED` est toléré seulement pendant la fenêtre de réveil bornée du
runner : après `starts_at_utc + 35 s`, il est affiché `PREPARED_STALE`, la
readiness devient non verte et le moniteur s'arrête sur alerte sans inventer une
corruption d'intégrité.

B tolère de façon bornée une première connexion loopback refusée ou un 503 de
démarrage et publie un motif court sans traceback. Avant le moindre collecteur,
le verdict dashboard exige simultanément
le HTTP live readonly, `orders_enabled=false`, le PID/commande systemd exacts,
le `FragmentPath` exact et la preuve `/proc` que ce même PID possède l'unique
listener IPv4 `127.0.0.1:18081`. Un serveur étranger, un mauvais PID, une
commande/unité divergente, une preuve initiale invalide ou un délai dépassé ne
peut donc jamais produire GREEN; seul le dashboard de la nouvelle tentative est
alors arrêté/désactivé et les racines restent intactes.

Le moniteur refuse aussi la racine de campagne, `state/` et tout répertoire de
venue présent s'ils sont liés, spéciaux ou non canoniques. Cette règle précède
la lecture des preuves : un alias filesystem ne peut pas produire un faux GREEN.

Après ce gate, B fige les venues depuis ce même snapshot moniteur authentifié et
propage tout échec de parsing. Il ne relit pas l'incoming mutable pour décider un
démarrage. `eligible_venues=[]` reste admis comme `BOTH_UNAVAILABLE` explicite,
dashboard-only, sans faux collecteur ni faux métrique.

Avant d'activer le dashboard ou un collecteur, B exécute les deux unités oneshot
namespace et exige pour chacune `Result=success`, exit 0 et zéro restart. Un
refus expose de façon bornée `TARGET`, `SOURCE`, `FSTYPE`, `VFS_OPTIONS`,
`MAJ:MIN`, `FSROOT` et le chemin logique. Si un probe ou un collecteur échoue
avant un state authentifié, B capture immédiatement Result, exit, présence
state/ledger, monitor JSON et journal borné, puis arrête/désactive les trois
services persistants et les deux probes du nouveau slug. Il ne boucle plus sur
un service déjà terminal et ne laisse plus le dashboard isolément actif.

Le recovery applique les mêmes preuves exactes de fragment au dashboard et aux
collecteurs, plus la possession du listener dashboard. Sa fermeture exige une
admission globale sans panne opérationnelle. Une alerte
`PUBLIC_SOURCE_INVALID` authentique reste admissible et comptabilisée; une
divergence de fragment, listener, commande ou état ne peut pas produire le signal
de reprise.

Ses scénarios synthétiques couvrent aussi les terminaux invalides propres à
chaque venue, les deux venues invalides, les états mixtes invalid/unavailable et
`CAPACITY_REFUSED`, sans masquer le dernier état terminal courant.

Le protocole complet, la création du bundle, les cinq fichiers opérateur et les
règles recovery/rollback sont dans
[`ops/prediction_markets_launch_v1/README.md`](../ops/prediction_markets_launch_v1/README.md).

## Frontière et limites

Tout reste `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`. Aucun wallet, signer, seed,
secret, compte/API privée, ordre, annulation/modification, route réelle, proxy ou
contournement n'a été ajouté. Les fixtures cockpit sont
`SYNTHETIC/FIXTURE — NOT ALPHA OR ECONOMIC EVIDENCE`.

Le checkout prouve la préparation logicielle et les gates offline. Il ne prouve
pas l'accès futur depuis le VPS, la capacité future, un corpus complet, un alpha,
une rentabilité ou une autorisation de trading. Ces faits attendent l'exécution
humaine et les preuves prospectives publiques.
