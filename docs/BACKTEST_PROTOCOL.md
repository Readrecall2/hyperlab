# Protocole de backtest — Phase 04

## Statut et frontière de sécurité

Le moteur est un cadre de recherche local. Il ne contient aucun transport vers une
venue, aucune authentification, aucun signataire et aucune capacité d'envoyer,
modifier ou annuler un ordre. Les objets maker, taker et IOC sont uniquement des
événements simulés.

Le jeu local disponible le 12 août 2026 ne satisfait pas la Gate B : il ne contient
pas trente jours propres et ne couvre pas simultanément Hyperliquid et la venue de
référence. Une exécution sur les fixtures ou le générateur de démonstration porte
donc obligatoirement le statut `SYNTHETIC`; une hypothèse sans calibration porte
`UNCALIBRATED`. Aucun rapport Phase 04 ne constitue une preuve de rentabilité.

## 1. Chaîne de recherche obligatoire

```text
lake immuable et validé
→ vue point-in-time
→ plan train/validation/test hashé
→ variantes enregistrées avant exécution
→ walk-forward train passé / validation OOS
→ variante figée
→ révélation unique du test final
→ simulation d'exécution et ledger
→ stress, bootstrap et rapport
```

La commande `backtest` matérialise cette chaîne et laisse le test final verrouillé par
défaut. Son dossier CSV n'est accepté que s'il contient aussi
`depth_usd.csv`, `available_at.csv`, `finality.csv`, `tradable.csv` et
`metadata.json` avec `point_in_time=true`, `historical_universe_source` et un
`lifecycle_hash` SHA-256. Le chemin CLI échoue fermé si ces contrôles ou la profondeur
sont absents. `--reveal-final` autorise la révélation unique uniquement après la
sélection et le gel enregistrés.

Dans cet export aplati, une cellule `available_at[t, instrument]` est le maximum des
`received_time` de **tous** les champs non nuls exposés pour cet instrument à `t`
(prix, funding, spread, volume et profondeur). `finality[t, instrument]=true` signifie
que toutes les observations de bougie correspondantes ont franchi leur politique de
finalité. Le producteur de l'export doit calculer ces agrégats depuis les événements
bruts point-in-time; choisir seulement l'heure de réception du prix violerait le
contrat et invaliderait le run.

## 2. Temps, disponibilité et finalité

Une observation utilisable à une décision `t` satisfait toutes les conditions :

- `received_time <= t` ;
- pour une bougie, `close_time <= t` ;
- `is_final` n'est jamais `false` ;
- une finalité source inconnue (`null`) suit une politique et un délai préenregistrés ;
- l'âge ne dépasse pas le seuil de staleness ;
- l'instrument appartient à l'univers historique connu à `t`.

`select_candle_revisions_as_of` conserve la dernière révision éligible réellement reçue,
sans relire rétroactivement une correction future. `join_venues_as_of` effectue une
jointure backward sur le temps de réception, mesure séparément l'âge source et l'âge
de réception, puis applique la staleness au temps source. Il rejette un événement
source situé après sa réception ou après la décision. `universe_mask_as_of` reconstruit `listed`, `renamed` et
`delisted`; les actifs délistés restent des colonnes historiques afin d'éviter le
survivorship bias.

L'absence de lifecycle rend invalide toute sélection dynamique d'univers. Aucun
forward-fill silencieux n'est autorisé.

## 3. Causalité bar-level

Le signal décidé à `t` est transformé en ordre simulé à `t` ou plus tard, selon la
latence. La position remplie commence à gagner sur `t → t+1`. Le moteur conserve
séparément :

- `target_weights`, les positions demandées ;
- `weights`, les positions effectivement remplies ;
- `fills`, tous les ordres remplis, partiels, manqués, expirés ou IOC ;
- `returns`, les composantes de PnL ;
- `attribution`, le ledger long réconcilié.

Les index et colonnes du signal doivent correspondre exactement au panel. Un
décalage ne devient jamais silencieusement une position nulle. Les tests
métamorphiques modifient le futur de chaque baseline et exigent que le préfixe des
décisions reste identique.

Le mark-to-market `t-1 → t` précède les décisions et fills de `t`. Les positions
effectivement détenues dérivent avec les prix et les coûts : une cible inchangée ne
provoque ni rebalancement implicite gratuit, ni turnover caché. Un ordre latent devenu
obsolète est annulé avant son éventuelle exécution à `t`.

## 4. Split verrouillé et walk-forward

`SplitPlan` définit trois intervalles UTC non chevauchants `[start, end)` et lie le
plan à un hash de dataset. Sa sérialisation JSON canonique produit un hash stable.

Le code de sélection reçoit uniquement `SelectionSplitView`, qui expose train et
validation, jamais les dates du test final. `FinalTestLock` impose :

1. sélection sur train/validation ;
2. gel du hash exact d'une variante ;
3. émission d'un jeton lié au plan et à la variante ;
4. révélation unique de la plage de test final.

`WalkForwardSpec` génère des fenêtres rolling ou expanding. Le train précède la
validation, l'embargo est explicite, les segments sont non vides et les fenêtres OOS
ne se chevauchent pas. Le workflow complet exige en plus des fenêtres OOS contiguës,
afin que les blocs bootstrap ne traversent ni ne compressent une lacune temporelle.
`run_walk_forward` appelle la calibration uniquement avec le panel train, puis exécute
la validation suivante. Les rendements OOS sont concaténés une seule fois et dans
l'ordre chronologique.

## 5. Registre de variantes

`ResearchRegistry` écrit du JSONL append-only avec numéro de séquence et chaîne de
hashes. Chaque mutation est protégée par un verrou interprocessus et le head attendu
(séquence, hash, taille) est ancré dans un sidecar remplacé atomiquement. La suppression
d'une ligne terminale complète est donc détectée, contrairement à une chaîne de hashes
isolée. Un crash entre l'append et le sidecar échoue fermé et exige un audit manuel.
Le plan `plan_created` est inscrit avant la première variante; une variante est inscrite
avant son résultat. Le registre conserve :

- stratégie et paramètres complets ;
- scénario et seed ;
- hashes code, données et split ;
- métrique de sélection et direction ;
- succès, perte, erreur ou interruption ;
- métriques négatives et nombre total d'essais.

Une modification ou troncature est détectée lors de la relecture. La sélection recharge
son événement OOS source et vérifie métriques, plan, dataset, split et hash; un résultat
forgé est refusé. Les objectifs sont limités à une liste de métriques sans valeur cible,
et les alias camelCase/imbriqués de cible de rendement sont refusés. Une
`SelectionObjective` contient seulement une métrique et une direction : aucun rendement
cible, seuil mensuel, cap ou attraction vers une valeur souhaitée.

## 6. Frais, profondeur et slippage

`CostSchedule` recherche une règle as-of par venue/instrument et période d'effet.
Un instrument sans règle échoue fermé. Les frais maker sont signés afin de conserver
un éventuel rebate; un stress adverse ne peut jamais augmenter ce rebate.

Pour un notionnel `Q` et une profondeur exécutable `D`, le modèle calcule :

```text
participation = Q / D
slippage_bps = base_bps + coefficient × participation^exposant
capacité = max_participation × D
fill_fraction = min(1, capacité / Q)
```

Le slippage croît donc avec la taille et lorsque la profondeur diminue. Dépasser la
capacité crée un fill partiel et un reliquat explicite. Le volume quotidien ne doit
pas être présenté comme profondeur exécutable.

Les valeurs de `config/research.toml` sont des placeholders `UNCALIBRATED`. Le code
refuse le statut `CALIBRATED` sans un `calibration_evidence_hash` SHA-256 et une source
ou un identifiant de méthode non-placeholder. Elles ne deviennent donc `CALIBRATED`
qu'avec un échantillon de replay/fills versionné et hashé.
Le moteur échoue fermé si ce statut est déclaré sans un
`calibration_evidence_hash` SHA-256 minuscule pour les données, le `CostSchedule` ou
le `MakerFillModel`. Il refuse également une `calibration_source`, une source de règle
de coût ou un `calibration_id` vide, synthétique, par défaut ou explicitement placeholder.
Ces trois hashes sont recopiés dans les diagnostics et donc dans les artefacts du run.
Un hash établit l'identité de la preuve déclarée, pas sa qualité :
la Gate C exige encore l'audit du contenu, de la méthode et de la couverture de
l'échantillon avant toute conclusion économique.

## 7. Maker, jambes et IOC

Le modèle maker est calibrable par probabilité de base, décroissance avec la
participation, identifiant de calibration et seed. Un ordre éligible peut rester
`NO_FILL`; `PARTIAL`, `CANCELLED` et `EXPIRED` sont aussi des états de premier rang.
Même seed et mêmes intentions donnent les mêmes tirages.

Les groupes de hedge ordonnent leurs jambes. `leg_delay_bars` retarde chaque jambe
suivante; l'exposition transitoire et son PnL sont classés dans `hedge_return`.
Après le timeout d'un maker, `emergency_ioc` ne crée un IOC purement simulé que pour
réduire/aplatir une exposition ou combler une jambe de hedge déjà ouverte. Une entrée
maker ratée n'est jamais poursuivie en taker. L'IOC paie spread, frais taker, slippage
et pénalité d'urgence; il peut être partiel ou non rempli et laisse alors un reliquat
visible.

## 8. Identité du PnL

À chaque barre, l'identité additive est :

```text
net = prix + funding + basis + spread + frais + slippage + hedge
coûts = spread + frais + slippage
```

`basis` reçoit le PnL de prix d'un groupe opposé et complètement hedgé. `hedge`
reçoit le PnL de prix d'une exposition transitoire entre jambes. Ces catégories
remplacent, et ne doublent jamais, `price` pour les mêmes lignes.

Le ledger convertit chaque rendement en PnL avec le capital au début de la barre.
La somme par composante, instrument, actif et portefeuille doit réconcilier
exactement la courbe de capital. Le rapport fournit aussi exposition réelle,
turnover réel, fill rate, pire heure et pire jour.

## 9. Benchmark, incertitude et ventilations

Le benchmark est une série alignée sur le même calendrier et le même capital :

- cash/yield passif composé selon une hypothèse sourcée ; ou
- buy-and-hold d'un instrument présent sur toute la fenêtre.

Ce benchmark sert à comparer après mesure; ce n'est jamais une cible d'optimisation.

`block_bootstrap_ci` rééchantillonne des blocs temporels contigus avec seed,
longueur de bloc, nombre de réplications et niveau de confiance enregistrés. Un
échantillon trop court reste calculable mais déclenche un avertissement visible.
L'index doit être UTC, régulier, ordonné et sans trou : un bloc ne traverse jamais une
lacune non observée. Le bootstrap porte uniquement sur les rendements explicitement
tagués OOS; la démo in-sample affiche `UNAVAILABLE_NOT_OOS`.

Les rapports réconcilient le PnL par :

- actif, y compris les actifs ensuite délistés ;
- mois UTC ;
- régime calculé uniquement à partir du passé ;
- tranche de taille/notionnel.

## 10. Scénarios obligatoires

`run_stress_matrix` conserve les intentions et seeds, puis applique au minimum :

- base ;
- coûts ×2 ;
- probabilité maker dégradée ;
- latence dégradée ;
- suppression des meilleurs 5 % de trades économiques clôturés.

La suppression regroupe les jambes d'un même hedge en un trade, tranche les ties de
façon déterministe, ne refit aucun paramètre et enregistre les identifiants retirés.
Le cash-and-carry ajoute un scénario propre d'inversion du funding (`× -1`). Les
scénarios de volatilité et panne de venue restent à ajouter au niveau de chaque
stratégie lorsqu'un dataset réel suffisant existe.

## 11. Limites restantes

- Une simulation bar-level ne démontre pas le market making ou le lead-lag rapide.
- La finalité Hyperliquid inconnue exige une politique de délai explicite; elle ne
  doit jamais être réécrite comme finalité source.
- L'impact paramétrique n'est pas un replay de carnet L2.
- La calibration des frais, de la profondeur, des fills et de la latence manque dans
  le dataset local actuel.
- Le verrou final, son gel et sa révélation sont audités dans le registre, mais restent
  une garde de protocole locale et non un système de contrôle d'accès hostile.

La Phase 04 peut donc valider le cadre et ses tests sur fixtures/synthétique, mais
elle ne franchit pas les Gates B ou C et ne valide aucune stratégie économique.
