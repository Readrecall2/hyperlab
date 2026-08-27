# Prediction Markets Prospective Launch V1

Verdict terminal logiciel :
`PREDICTION_MARKETS_PROSPECTIVE_LAUNCH_V1_GREEN_AWAITING_HUMAN_EXECUTION`.

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

Le troisième service est un cockpit séparé sur `127.0.0.1:18081`. Les trois
unités sont non-root, `ProtectSystem=strict`, `NoNewPrivileges`, sans capacité,
sans secret et avec surfaces d'écriture limitées aux racines de venue.

## Admission et cohabitation

Le preflight découvre le volume réel au lieu de supposer le volume Hetzner 200
GB. Il exige ext4 rw, fsync, NTP, CPython 3.12 x86_64 avec glibc >= 2.28,
imports offline, wheelhouse multi-tags `manylinux_2_28` + `manylinux_2_17`
hashé, Git et bundle exacts, port 18081 libre, racines/services absents et
connectivité publique officielle bornée.

La cohabitation réserve 144 GiB pour H1, 21 GiB pour les 1 344 shards Prediction
Markets et 16 GiB de marge, soit 181 GiB libres. Si cette marge n'est pas prouvée,
le pack refuse et recommande un hôte ou volume distinct sans toucher à H1.

Une venue DNS/HTTPS/WSS indisponible reçoit son propre verdict; l'autre venue et
le cockpit restent installables. Kalshi WSS n'est jamais sondé car le contrat
documenté exige une authentification; la collecte Kalshi V1 reste REST publique.
Toute reprise revalide d'abord NTP, ext4 `rw`, capacité, racines et imports
offline, puis refait le preflight public borné venue par venue avant
réactivation; un nouvel échec reste archivé et ne bloque pas l'autre collecteur.

## Démarrage et observation

Le quick-start fixe la campagne à l'heure UTC d'activation après réussite de
l'installation. L'opérateur peut fournir un `HYPERLAB_PM_START_AT_UTC` explicite,
borné à +24 h. Le pack ne contient donc aucune date future artificielle.

Le cockpit expose uniquement GET/HEAD, `mode=readonly` et
`orders_enabled=false`. Il conserve le holdout `SEALED`, refuse les lectures
filesystem non sûres et affiche `NON DISPONIBLE` pour toute métrique absente.

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
