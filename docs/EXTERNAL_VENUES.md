# Venue de référence publique — Binance USDⓈ-M Futures

Dernière vérification documentaire : 13 août 2026.

## Choix et périmètre de sécurité

La première venue externe est **Binance USDⓈ-M Futures**, limitée aux contrats
perpétuels linéaires `BASEUSDT` dont l'identité figure dans `exchangeInfo`.
BTCUSDT et ETHUSDT apportent une référence liquide pour le funding
cross-exchange, le lead-lag et la fair value. Ce choix n'affirme ni équivalence
économique parfaite avec Hyperliquid, ni disponibilité dans toutes les
juridictions.

`BinancePublicRestClient` ne sait exécuter que des requêtes HTTP `GET` sur une
allowlist fermée : heure serveur, métadonnées, BBO, trades agrégés, funding et
candles. Il ne possède aucun paramètre de clé, signature ou compte. Toute route
ordre, position, compte, listen key, transfert ou authentification est rejetée
avant le transport. Le WebSocket utilise uniquement les flux publics Binance :
`/public/stream` pour BBO/L2 et `/market/stream` pour `aggTrade`, mark et
candles. Ces deux sockets sont supervisés comme une seule génération de capture.

Commande de collecte simultanée avec Hyperliquid :

```powershell
.\.venv\Scripts\python.exe -m hyperlab collect-multi-venue `
  --assets "BTC,ETH" `
  --candle-intervals 1m `
  --duration-seconds 600
```

Cette commande est la voie sûre pour capturer les deux venues dans le même lake :
un seul writer détient le root lock et sérialise les deux producteurs. La
commande `collect-reference` reste utilisable pour Binance seule, mais ne doit
pas être exécutée en parallèle de `collect` sur la même racine.

Les données Binance vont dans le lake immuable sous `venue=binance_usdm` et
Hyperliquid sous `venue=hyperliquid`. L'état Binance séparé se trouve dans
`data/runtime_status_binance_usdm.json` et apparaît aussi avec
`hyperlab status`. Voir [`MULTI_VENUE_COLLECTION.md`](MULTI_VENUE_COLLECTION.md)
pour l'audit du writer, la commande longue et les contrôles post-capture.

## Flux collectés

| Besoin | Surface publique | Normalisation |
|---|---|---|
| identité et tailles | `GET /fapi/v1/exchangeInfo` | `BTCUSDT → BTC`, linéaire, quantité en BTC, tick/step conservés |
| BBO | `/public/stream` : `<symbol>@bookTicker` | prix USDT par base, quantités en unité de base, `u` conservé |
| L2 | `/public/stream` : `<symbol>@depth20@100ms` | snapshot top-20 complet, deux côtés, dernier update ID, epoch de carnet et niveaux ordonnés |
| trades | `/market/stream` : `<symbol>@aggTrade` | ID agrégé, prix, quantité de base, côté agresseur dérivé de `m` |
| funding courant | `<symbol>@markPrice@1s` | taux conservé ; index présent seulement dans le wire brut |
| funding réalisé | `GET /fapi/v1/fundingRate` | règlement et mark Binance ; cadence ajustée issue de `fundingInfo`, sinon minimum observé sur au moins deux règlements |
| candles | REST `klines` + `<symbol>@kline_<interval>` | OHLCV, révisions et finalité source WebSocket |
| horloge | `GET /fapi/v1/time`, toutes les 5 s | RTT, drift par midpoint, incertitude `RTT / 2` et validité causale bornée |

Les payloads WebSocket bruts sont conservés avec horodatage de réception,
connexion physique, epoch, séquence d'arrivée et génération de capture. Chaque
trade normalisé conserve les temps `T`/`E`, le temps de réception et la lignée
physique de son wire brut. Les autres lignes normalisées conservent aussi le
timestamp source lorsqu'il existe. La latence réseau corrigée est une
**estimation** : `received - source + (local_midpoint - server)`. Elle reste
signée et n'est jamais ramenée artificiellement à zéro.

Le snapshot top-20 est enregistré sans fabriquer les niveaux absents et sans le
transformer artificiellement en delta. Le premier snapshot de chaque actif après
génération produit `resync_start` et `resync_complete`, associés à son
`book_epoch_id` et à son `snapshot_id`. Les snapshots exacts suivants ne
rafraîchissent la couverture que dans cette même connexion publique, epoch et
carnet déjà armés. Les flux BBO et L2 sont surveillés
séparément pour chaque actif, tout comme `aggTrade` ; leur silence au-delà du
seuil de staleness provoque un gap historique, la fermeture des deux sockets et
une reconnexion commune, même si d'autres canaux continuent à recevoir des
messages.

Une mesure d'horloge ne devient causale qu'à sa réception. Avec les seuils par
défaut, `drift_uncertainty_ms <= 50` produit l'intervalle semi-ouvert
`[response_received_time, response_received_time + 15 s)`. Une incertitude plus
forte, une mesure stale, une déconnexion ou un changement de génération coupe la
couverture ; aucune interpolation ne relie ces périodes. Les anciennes lignes
`clock_sync` v1 restent lisibles mais ne fournissent aucune couverture causale à
l'audit strict.

## Identité, mark, index et oracle

Un ticker identique ne suffit pas. Le connecteur admet seulement un perp linéaire
`BASEUSDT`, margé et coté en USDT, explicitement retourné par `exchangeInfo`.
Les contrats inverses COIN-M sont refusés : leur multiplicateur et leur unité de
taille diffèrent.

Le champ Binance `mark price` reste un mark Binance. Son `index price` n'est
**jamais** stocké dans `oracle_price`, car l'oracle Hyperliquid et l'index
Binance n'ont ni la même définition ni nécessairement les mêmes constituants.
L'index demeure auditable dans le wire brut. Une future fair value doit traiter
chaque définition comme une composante distincte.

## Replay synchronisé et qualité

`replay_synchronized` fusionne les captures par `received_time`, puis par venue,
epoch et séquence d'arrivée. Il n'utilise jamais un timestamp exchange pour
réordonner ce que le collecteur a réellement observé. Il signale :

- régression du temps de réception dans une capture ;
- temps source hors ordre, par venue et canal ;
- spread de réception multi-venue supérieur au seuil ;
- silence prolongé d'une venue pendant que l'autre avance ;
- trou propre à une venue et reprise après absence.

Les coupures, erreurs de transport et flux stale alimentent l'état runtime
`maintenance_or_absence_detected`. Cela détecte une indisponibilité observable,
mais ne permet pas toujours de distinguer maintenance annoncée, panne réseau
locale, filtrage régional ou panne de la venue. Le lake détecte également les
trous de funding à partir de la cadence publiée, sans forward-fill. Les
connexions, déconnexions et gaps stale sont conservés comme
`connection_event`, afin que les périodes d'absence restent historiques.
Un instrument dont le statut `exchangeInfo` n'est pas `TRADING` arrête le
bootstrap et marque immédiatement la venue indisponible.

## Limites et conditions d'utilisation

- Binance peut modifier formats, limites, cadences de funding et endpoints ;
  vérifier la documentation avant chaque déploiement.
- Les limites sont par IP et peuvent provoquer `429`, puis `418` si le client ne
  respecte pas le backoff. La commande limite volontairement son bootstrap.
- Une connexion market stream peut être interrompue ; la reprise crée une
  nouvelle epoch et un resync L2 explicite, sans prouver qu'aucun trade ou
  événement intermédiaire n'a été manqué.
- Les trades sont **agrégés par ordre taker**, pas des fills individuels.
- Le top-20 n'est pas le carnet complet au-delà de cette profondeur et ne prouve
  aucune possibilité de fill.
- Une candle REST en cours n'est pas déclarée finale avec l'horloge locale.
- L'historique REST est borné et paginé ; un collecteur 24/24 reste nécessaire.
- `fundingInfo` ne publie que les ajustements. Sans ajustement publié, la cadence
  est mesurée sur au moins deux règlements adjacents ; elle n'est jamais fixée
  silencieusement à huit heures. Un historique trop court arrête le bootstrap.
- La disponibilité de Binance et des dérivés dépend de la juridiction. Les
  conditions Binance, les règles locales et les droits de réutilisation des
  données doivent être vérifiés par l'opérateur.
- Aucun SLA, aucune garantie d'exactitude et aucune équivalence de contrat entre
  venues ne sont supposés.

## Sources officielles

- Catalogue USDⓈ-M : https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api
- Market streams USDⓈ-M : https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams
- Partial book depth : https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
- Documentation développeur : https://developers.binance.com/en/docs/introduction
- Conditions Binance : https://www.binance.com/en/terms
- Données historiques publiques : https://data.binance.vision/
