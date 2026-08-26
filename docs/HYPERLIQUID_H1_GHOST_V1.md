# Hyperliquid H1 Ghost V1 — one-sided selective maker

Status technique de l’implémentation :
`HYPERLIQUID_H1_GHOST_V1_READY_FOR_PROSPECTIVE_EVIDENCE`.

Statut économique initial et obligatoire :
`ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE`.

Cette distinction est permanente : le verdict technique signifie que la
politique, le replay, les contrôles et le runner sont prêts à recevoir une
collecte prospective. Il ne signifie ni alpha, ni rentabilité, ni capacité, ni
autorisation de trading réel.

## Frontière

Le chemin H1 est strictement
`PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`. Il n’importe pas le SDK Exchange, ne
configure aucune adresse opérateur, ne lit aucun canal utilisateur, n’utilise
aucun compte, wallet, secret ou signer, et ne possède aucune route de création,
modification ou annulation réelle. Les éventuels champs opaques publiés dans le
wire public `trades` restent dans la preuve raw authentifiée mais ne sont jamais
lus, projetés, reportés ou utilisés par H1.

Le replay consomme seulement un `ResearchSegmentReader` ouvert avec le SHA-256
explicite d’un manifest. Les segments, leur chaîne, les séquences d’arrivée et
la racine raw sont authentifiés avant toute décision. Un mélange de provenance
publique et synthétique est refusé. Une provenance publique doit en plus lier
exactement les endpoints HTTP/WebSocket Hyperliquid officiels et la version de
métadonnées publique épinglée ; une URL seulement syntaxiquement publique ne
suffit pas.

## Préenregistrement

La politique est figée dans
`config/research/hyperliquid-h1-ghost-v1.json`. Son identité est le SHA-256 de
sa représentation JSON canonique, calculé et enregistré dans chaque rapport et
manifest de campagne.

Avant la première observation prospective, le fichier fixe :

- l’univers perp `BTC`, `ETH`, `SOL`, `HYPE`, sans sélection par PnL futur ;
- les transformations point-in-time : profondeur top 3, imbalance, microprice,
  flux agressif reçu sur 5 s et régime backward-looking sur 30 s ;
- une variante primaire et deux variantes perdantes/prudentes enregistrées,
  toutes avec holdout `SEALED` ;
- les règles exactes `BID_ONLY`, `ASK_ONLY` et `NO_QUOTE` ;
- un notional cible de 50 USDC, limité à 10 % de la taille affichée, arrondi au
  lot point-in-time ;
- TTL 2 s, une seule quote active par instrument, inventaire maximal de
  100 USDC et closeout sur dernière profondeur fraîche finie ;
- les seuils de stale, contexte, profondeur, spread, funding boundary, risques
  et kill rules ;
- les splits UTC chronologiques train J0–J7, validation J7–J10, holdout
  J10–J14 ;
- les gates économiques futurs et la configuration du runner.

Les variantes ne peuvent pas être reclassées après exposition au holdout. Le
runner fige de nouveau politique, frais, univers, fenêtres et hashes dans
`campaign-manifest.json` avant toute collecte.

## Politique causale

Une décision est créée seulement sur un snapshot L2 reçu. Elle utilise
exclusivement des métadonnées, BBO, contexte actif et trades dont le temps de
réception est antérieur ou égal à la décision. Le temps source n’est jamais
substitué au temps de connaissance local.

Les grilles de prix et de quantité sont versionnées au moment de chaque L2 à
partir des métadonnées déjà reçues et de la précision observée jusque-là. Une
métadonnée future ne peut ni valider un ancien book ni modifier rétroactivement
un ordre ; une grille absente ou ambiguë ferme la fenêtre.

La politique émet au plus un côté :

- `BID_ONLY` lorsque imbalance, microprice et flux agressif reçu confirment le
  côté bid ;
- `ASK_ONLY` pour le miroir négatif ;
- `NO_QUOTE` dans tous les autres cas.

`NO_QUOTE` est obligatoire sur gap/reconnect non resynchronisé, book/BBO/contexte
stale ou absent, divergence BBO/L2, profondeur finie insuffisante, coût ou
funding non résolu, proximité du règlement horaire, signal non sélectif, taille
inférieure au lot, quote déjà active ou inventaire hors limite. Un reconnect ne
rouvre la fenêtre qu’après de nouveaux BBO, contexte et L2 dans la nouvelle
génération.

## Exécution Ghost réutilisée

H1 étend `Base Realism / Ghost V1`; il n’introduit pas de second moteur de
queue, coûts, profondeur ou closeout.

Chaque action devient une ALO hypothétique après la latence du scénario. Le
contact ne remplit jamais. Toute quantité affichée au prix lors de l’admission
est initialement devant :

- primaire pessimiste : crédit cancellation-ahead 0 %, multiplicateur 1.00 ;
- secondaire conservateur : crédit maximal 25 %, multiplicateur 0.75 ;
- sensibilité non promotable : crédit maximal 50 %, multiplicateur 0.50.

Seuls les trades agressifs publics reçus après l’ack consomment la queue. Le
moteur conserve partials, missed fills, rejet ALO marketable, cancel/fill race,
perte de priorité, limite d’inventaire et closeout à profondeur exécutable. Une
profondeur ou une santé insuffisante laisse un reliquat visible au lieu de
l’inventer.

Les scénarios figés sont :

| Latence | Rôle |
|---:|---|
| 100 ms | frontière, jamais suffisante seule |
| 250 ms | diagnostic |
| 500 ms | hurdle principal |
| 1 000 ms | stress |

Un résultat favorable seulement à la frontière 100 ms ne peut jamais promouvoir
HyperLab. Le gate économique principal reste la LCB95 nette positive à 500 ms,
rebate primaire zéro.

## Économie et rapports

Le coût primaire utilise le tier public standard prudent : maker 1,5 bps,
taker/closeout 4,5 bps, aucun tier privilégié, discount ou rebate. Le manifest
de campagne exige une revue humaine de l’artefact public dans les 24 heures
avant le début et en épingle les octets exacts.

Chaque décision expose les markouts signés à 100 ms, 500 ms, 1 s, 5 s, 30 s et
120 s. Une observation absente ou reçue plus de 250 ms après sa cible reste
`null`; elle n’est ni forward-fill ni extrapolée. Le fill-to-close est calculé seulement sur un round-trip
conservateur effectivement apparié.

L’attribution sépare spread, signal, fees, adverse selection, inventory,
funding, forced close, opportunity cost et rebate. Le réalisé/non réalisé est
publié seulement lorsque le closeout est résolu; sinon ces champs restent
explicitement non certifiables. Les composantes se réconcilient exactement.

Aucune moyenne globale n’est présentée sans les concentrations par marché, jour
UTC et événement. Le rapport publie séparément le nombre de fills conservateurs
et les appariements d’inventaire terminés. Pour la LCB95 et le top 1 %, le PnL
de chaque appariement maker/maker est partagé symétriquement entre ses deux
fills ; un closeout forcé reste attribué au fill maker ouvert. Il publie aussi
le p99 d’inventaire notionnel et le p99 de slippage de closeout.

Les gates prospectifs exigent simultanément au minimum 5 000 fills
hypothétiques conservateurs, trois marchés, trois régimes, LCB95 nette positive
à 500 ms avec rebate zéro, top 1 % non dominant, p99 inventaire acceptable,
p99 closeout acceptable, closeout résolu, provenance non synthétique et
réconciliation exacte. Même un passage demande encore revue humaine et ne crée
aucune route réelle.

## CLI offline H1

Le replay ne démarre aucun transport :

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab ghost h1-replay `
  --research-root 'C:\path\campaign\raw' `
  --manifest-sha256 '<sha256-explicite>' `
  --config '.\config\research\hyperliquid-h1-ghost-v1.json' `
  --output '.\reports\ghost\hyperliquid-h1-ghost-v1.json'
```

Le petit probe Hyperliquid existant est admissible uniquement comme smoke
technique. Sa durée d’environ 58 s et ses 716 frames ne sont jamais promues en
preuve économique. Aucun nouveau probe n’a été lancé pour cette implémentation.

À partir de J13 seulement, l’opérateur peut produire un rapport offline sur un
préfixe manifesté et l’écrire sous
`state/verified-threshold-report.json`. Le runner n’accepte un arrêt anticipé
que si ce rapport lie exactement la politique et un préfixe raw authentifié,
est non synthétique, contient le scénario 500 ms, et si **tous** ses gates sont
vrais. Le runner recalcule le rapport depuis le raw au lieu de faire confiance
au JSON déposé. Après fermeture du tail, il recalcule encore sur le manifest
final : seul ce second passage simultané produit
`COMPLETE_VERIFIED_THRESHOLDS`. Si le tail final invalide un gate, l’état est
`THRESHOLD_CANDIDATE_NOT_FINAL_RESUME_REQUIRED` et la collecte reste reprenable.
Cette règle séquentielle et la durée minimale de trois jours de holdout sont
figées avant collecte; un fichier invalide, partiel ou prématuré est simplement
ignoré.

## Préparation de la campagne future

La préparation est locale, non interactive et ne démarre aucun réseau :

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab research-data h1-prepare `
  --campaign-root 'D:\hyperlab-evidence\hyperliquid-h1-001' `
  --starts-at-utc '2026-09-01T00:00:00Z' `
  --fee-reviewed-at-utc '2026-08-31T23:00:00Z' `
  --config '.\config\research\hyperliquid-h1-ghost-v1.json' `
  --fee-artifact '.\config\paper\hyperliquid-tier0-fees-2026-08-16.json'
```

Signal terminal : `campaign-manifest.json` et son pin SHA-256. La commande
refuse un répertoire existant, un début déjà passé, un timestamp sans fuseau,
une revue future ou postérieure au démarrage, ou une revue vieille de plus de
24 heures.

Le répertoire produit deux blocs opérateur prêts à relire :
`operator/windows-powershell.txt` et `operator/tabby-vps-bash.txt`.

## Bloc opérateur Windows — collecte future seulement

- lieu : Windows PowerShell local ;
- durée attendue : 7–14 jours ; maximum : 14 jours plus finalisation bornée ;
- prompts : aucun ;
- monitoring sûr : lire `state/health.json` depuis une seconde console ;
- Ctrl+C : ferme le tail admis et donne `INTERRUPTED_RECOVERABLE` ;
- reprise : même commande avec `--resume` ;
- fin : `COMPLETE_COLLECTION_WINDOW` ou seuils vérifiés dans le health final.

```powershell
& '.\.venv\Scripts\python.exe' -m hyperlab research-data h1-collect `
  --campaign-root 'D:\hyperlab-evidence\hyperliquid-h1-001' `
  --config '.\config\research\hyperliquid-h1-ghost-v1.json'
```

Monitoring, seconde console :

```powershell
Get-Content -LiteralPath 'D:\hyperlab-evidence\hyperliquid-h1-001\state\health.json' -Wait
```

## Bloc opérateur Tabby — VPS, à exécuter humainement plus tard

Codex ne lance aucune de ces commandes. Avant exécution, l’opérateur doit
revoir les chemins, l’espace disque, l’heure UTC, l’artefact de frais et le
manifest épinglé.

- lieu : `Tabby - VPS`, Bash ;
- durée attendue : 7–14 jours ; maximum : 14 jours plus finalisation bornée ;
- prompts : aucun ;
- monitoring sûr : deuxième onglet Tabby, lecture du health seulement ;
- Ctrl+C : tail authentifié fermé, état reprenable ;
- fin : health terminal et manifest SHA-256 explicite.

```bash
.venv/bin/python -m hyperlab research-data h1-collect \
  --campaign-root '/srv/hyperlab/evidence/hyperliquid-h1-001' \
  --config './config/research/hyperliquid-h1-ghost-v1.json'
```

Monitoring, second onglet Tabby :

```bash
watch -n 10 -- cat /srv/hyperlab/evidence/hyperliquid-h1-001/state/health.json
```

La collecte est mono-writer, append-only, manifestée, monitorable et
reprenable. La reprise conserve `campaign_id`, configuration, univers, splits,
holdout, frais et chaîne de segments; elle crée une nouvelle génération de
session/reconnect visible et ne fabrique aucune séquence serveur.

Le paquet opérateur Linux/systemd reproductible de la première campagne est
documenté dans `docs/H1_PROSPECTIVE_CAMPAIGN_LAUNCH_PACK_V1.md`. Sa préparation
reste locale et offline; son exécution VPS et le démarrage de la collecte sont
exclusivement humains.
