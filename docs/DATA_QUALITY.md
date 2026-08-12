# Qualité et audit des données

La couche de données est locale, immuable et strictement en lecture seule du
point de vue des marchés. Elle ne contient ni authentification, ni clé, ni
capacité d'envoyer un ordre.

Parquet et DuckDB font partie de l'installation standard :

```powershell
.\.venv\Scripts\python.exe -m pip install -e .
```

## Valider

```powershell
.\.venv\Scripts\python.exe -m hyperlab data validate data/lake
.\.venv\Scripts\python.exe -m hyperlab data validate data/lake `
  --date 2026-08-11 `
  --report data/quality/2026-08-11.json `
  --json
```

La validation vérifie le SHA-256 avant de lire le Parquet, puis le schéma et sa
version, le chemin de partition, les bornes temporelles UTC, le nombre de
lignes, les clés dupliquées, l'ordre, les trous déclarés et les séquences L2.
Elle ne remplit, ne trie et ne réécrit aucune donnée source.

La destination de `--report` doit être extérieure à la racine du lac passée à
la commande. Cette frontière interdit notamment de remplacer un Parquet, un
manifeste ou `catalog.duckdb` par un rapport. Hors du lac, un rapport existant
peut être remplacé atomiquement via un fichier temporaire au nom unique.

Une corruption produit un message stable et un code de sortie `2`, sans
traceback utilisateur :

```text
CORRUPT_PARTITION [hash_mismatch] partition=<chemin-relatif> expected_sha256=<hash> actual_sha256=<hash>
```

Le code de sortie est `0` uniquement lorsque toutes les partitions sélectionnées
sont valides. Les états `degraded` et `unobservable` restent des résultats
auditables et sont annoncés explicitement, sans message trompeur de réussite.
L'état quotidien `missing` écrit ou affiche le rapport, puis sort avec le code
`2` et le message stable `DATA_QUALITY [missing]`.

## Inventorier

```powershell
.\.venv\Scripts\python.exe -m hyperlab data inventory data/lake
.\.venv\Scripts\python.exe -m hyperlab data inventory data/lake --date 2026-08-11 --json
```

L'inventaire reconstruit `data/lake/catalog.duckdb` à partir des octets dont le
SHA-256 vient d'être vérifié et les matérialise dans des tables versionnées. Le
catalogue ne contient donc pas de vue paresseuse relisant ensuite un Parquet
potentiellement remplacé. Il affiche les partitions dans un ordre déterministe
et ne consulte pas une liste d'actifs actuellement cotés : les actifs délistés
restent visibles.

La sortie JSON contient un état synthétique : `missing` sans partition,
`degraded` si une partition ou un trou inter-segments est dégradé,
`unobservable` lorsque la détection de trous n'est pas possible, sinon `ok`.

## Exporter

```powershell
.\.venv\Scripts\python.exe -m hyperlab data export data/lake exports/btc-trades.parquet `
  --type trade `
  --venue HL `
  --asset BTC `
  --start 2026-08-01 `
  --end 2026-08-11 `
  --schema-version 1 `
  --format parquet
```

Les formats acceptés sont `parquet` et `csv`. Le suffixe du fichier doit
correspondre au format. Les bornes de dates sont inclusives. L'export valide
toutes ses sources, conserve leurs valeurs nulles sans modifier les partitions,
et refuse un fichier de destination existant : aucun écrasement implicite n'est
permis. La destination doit être extérieure au lac. `--schema-version` limite
l'export à une version positive exacte ; sans cette option, toutes les versions
compatibles avec les autres filtres sont considérées et l'export refuse une
sélection ambiguë couvrant plusieurs versions. La sortie est ordonnée
explicitement par dimensions puis selon les clés d'ordre du schéma, pour être
reproductible ; cet ordre de sortie ne réécrit jamais les partitions sources.

## Rapport quotidien reproductible

`data validate --date ... --report ...` écrit du JSON canonique UTF-8 : clés
triées, séparateurs stables, chemins relatifs et fin de ligne `LF`. Le document
n'inclut ni heure de génération, ni chemin absolu, ni information dépendant de
la machine. Deux validations des mêmes octets produisent donc exactement le
même rapport.

Le rapport contient la date UTC, l'état global, le nombre de partitions et de
lignes, les bornes temporelles, les trous, doublons, messages hors ordre,
resynchronisations, valeurs nulles, empreintes de schéma et hashes des fichiers.
Une partition invalide est signalée ; elle n'est jamais omise silencieusement.

## Contrats sémantiques

La validation refuse les prix nuls ou négatifs, les quantités, volumes et
notionnels négatifs, les relations OHLC ou BBO impossibles et les intervalles
temporels inversés propres aux bougies, frais et cycles de vie. Les quantités de
trade doivent être strictement positives. Les taux de funding et les frais
maker/taker restent signés : une valeur négative peut représenter un paiement
dans le sens opposé ou un rebate.

Les vocabulaires fermés sont versionnés avec le schéma : côtés L2 `bid`/`ask`,
agresseur `buy`/`sell`/`unknown`, action L2 `set`/`delete`, événements de
connexion `connect`/`disconnect`/`gap`/`resync_start`/`resync_complete`, type
d'instrument `spot`/`perp` et cycle de vie `listed`/`renamed`/`delisted`.
`rate_kind` et `scope` sont volontairement extensibles pour conserver les
libellés propres aux venues, mais ne peuvent pas être vides.

Toutes les lignes d'un même `snapshot_id` ou `update_id` L2 doivent partager
leurs métadonnées de séquence, époque, connexion et timestamps, y compris si
elles sont réparties entre plusieurs fichiers immuables. Cette vérification ne
suppose pas qu'un snapshot complet tient dans un seul fichier.

## Interprétation des trous

- Bougies, funding et autres flux cadencés : l'intervalle attendu est déclaré,
  jamais estimé depuis les lignes présentes.
- Trades et BBO : l'irrégularité temporelle est normale ; les identifiants et
  séquences disponibles servent à détecter les pertes.
- L2 : snapshots et deltas restent séparés. Toute reconnexion ou
  resynchronisation change explicitement l'époque de synchronisation.
- Un schéma Arrow dont les timestamps sont naïfs ou portent un fuseau autre
  que `UTC` est rejeté avant publication. Si un producteur a déjà converti une
  valeur naïve vers le type Arrow `timestamp[ns, tz=UTC]`, sa provenance n'est
  plus observable par cette couche : le producteur doit donc refuser cette
  valeur avant de construire la table.

Aucun de ces contrôles n'autorise un forward-fill. Une donnée manquante reste
visible dans le manifeste et dans l'export.

## Éligibilité au backtest

Une partition valide n'est pas automatiquement disponible pour une décision
historique. La couche Phase 04 impose ensuite `received_time <= decision_time`, la
clôture effective des candles, une politique explicite pour la finalité inconnue,
un seuil de staleness et l'appartenance au lifecycle connu à cette date. Cette couche
ne modifie jamais les données source et ne transforme jamais une révision tardive en
information disponible plus tôt.

Pour un export `MarketPanel`, `available_at` représente le maximum des temps de
réception de tous les champs non nuls de la cellule instrument/barre, et `finality`
est l'agrégat conservateur de leurs états d'éligibilité. Un export qui ne peut pas
reconstruire ces deux valeurs depuis les événements bruts ne peut pas être marqué
point-in-time.

## Limite de passage à l'échelle

La validation inter-segments lit un fichier à la fois, mais la vérification de
l'unicité exacte globale conserve les clés primaires en mémoire. Sa consommation
est donc `O(nombre d'événements)` et n'est pas bornée indépendamment du volume.
Avant une future indexation sur disque, un très grand historique L2 doit être
validé par fenêtres maîtrisées ou avec un outil externe disposant des ressources
adaptées.

Le catalogue DuckDB matérialise un snapshot vérifié et consomme donc de
l'espace disque en plus des Parquet sources. Il reste entièrement dérivé et
peut être supprimé puis reconstruit ; sa capacité doit être dimensionnée avant
de cataloguer un historique L2 volumineux.
