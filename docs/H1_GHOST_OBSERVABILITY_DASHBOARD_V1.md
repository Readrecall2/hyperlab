# H1 Ghost Observability Dashboard V1

Statut cible : `H1_GHOST_OBSERVABILITY_DASHBOARD_V1_GREEN_AWAITING_CAMPAIGN_BINDING`.

Le cockpit H1 est une surface locale strictement read-only. Il assemble un
snapshot borné depuis une seule racine de campagne explicitement configurée. Il
ne découvre aucun répertoire, ne démarre aucun collecteur ou replay, ne lance
aucune commande et ne possède aucune route d'ordre.

La bannière permanente est :
`GHOST ONLY · PUBLIC DATA · ORDERS IMPOSSIBLE`.

## Branchement explicite

Variables reconnues par `hyperlab serve` :

- `HYPERLAB_H1_CAMPAIGN_ROOT` : racine unique contenant
  `campaign-manifest.json`, son pin, `state/health.json`, `raw/` et les rapports
  publiés autorisés ;
- `HYPERLAB_H1_POLICY_CONFIG` : configuration H1 locale dont le hash doit être
  identique à celui du manifest de campagne ;
- `HYPERLAB_H1_FIXTURE` : fixture de démonstration utilisée seulement lorsque
  la racine réelle n'est pas configurée. Valeur par défaut :
  `PREPARED_NOT_STARTED`.

La racine de campagne doit être montée en lecture seule dans un conteneur. Le
dashboard refuse symlinks, fichiers spéciaux, dépassements de taille et chemins
hors contrat. Aucun chemin fourni par le client n'est transformé en chemin
filesystem.

## Prévisualisation Windows loopback

- lieu : Windows PowerShell local, à la racine du dépôt ;
- durée attendue : démarrage inférieur à 10 secondes, puis service continu ;
- durée maximale : aucune, jusqu'à arrêt volontaire ;
- prompts : aucun ;
- monitoring sûr : ouvrir `http://127.0.0.1:8000` ;
- effet de Ctrl+C : arrête seulement le dashboard read-only, sans modifier la
  campagne ni son collecteur ;
- signal de disponibilité : Uvicorn affiche qu'il écoute sur
  `http://127.0.0.1:8000` et `/health/live` retourne HTTP 200.

```powershell
$env:HYPERLAB_H1_CAMPAIGN_ROOT = 'D:\hyperlab-evidence\hyperliquid-h1-001'
$env:HYPERLAB_H1_POLICY_CONFIG = '.\config\research\hyperliquid-h1-ghost-v1.json'
& '.\.venv\Scripts\python.exe' -m hyperlab serve --host 127.0.0.1 --port 8000
```

Pour une démonstration synthétique avant campagne, ne définissez pas la racine
et choisissez un état dans le sélecteur de l'interface. L'étiquette
`SYNTHETIC/FIXTURE — NOT ALPHA OR ECONOMIC EVIDENCE` reste visible. Les URLs
`/?fixture=RUNNING_HEALTHY`, `/?fixture=STALE_RECONNECTING` et
`/?fixture=HOLDOUT_SEALED` sont adaptées à la revue visuelle.

## API H1 read-only

Toutes les réponses incluent `mode=readonly` et `orders_enabled=false`. Les
routes sont limitées à GET/HEAD :

- `/api/h1/snapshot` : snapshot cohérent complet ;
- `/api/h1/identity` : campagne, politique, frais, commit publié et hashes ;
- `/api/h1/collection` : frames, segments, octets, gaps, doublons du tail,
  reconnexions et génération ;
- `/api/h1/markets` : matrice BTC/ETH/SOL/HYPE par feed ;
- `/api/h1/strategy` : décisions, refus, intentions, fills et exposition
  réellement publiés ;
- `/api/h1/economics` : coûts, attribution et gates seulement quand ils sont
  certifiables ;
- `/api/h1/incidents` : timeline bornée ;
- `/api/h1/fixtures/{nom}` : fixture synthétique allowlistée ;
- `/api/h1/reports/{report_id}` : téléchargement allowlisté après ouverture
  canonique du holdout.

Les payloads absents restent `null` avec une présentation
`NON DISPONIBLE`. `HEAD_CHANGED_RETRY` est HTTP 409 et rejouable ; les échecs
d'intégrité ou de lecture restent fail-closed en HTTP 503.

## Cohérence et intégrité

Le lecteur vérifie avant et après assemblage l'identité des fichiers utilisés.
Il effectue au plus deux tentatives. Le manifest de campagne doit correspondre
à son pin, le manifest raw est validé avec `decode_manifest`, et le dernier
segment est validé avec `decode_segment`, y compris sa provenance Hyperliquid
publique officielle. Cette portée tail est explicitement affichée ; elle ne se
présente jamais comme un audit offline complet de tout l'historique.

Un rapport final est accepté seulement s'il est canonique, auto-hashé, non
synthétique et lié à la politique, au manifest raw, à la racine raw et à la
liste exacte des segments du head courant.

## Holdout et recherche

Le découpage est fixe : Train J0-J7, Validation J7-J10, Holdout J10-J14. Tant
que le health n'est pas dans un état terminal canonique, le holdout reste
`SEALED`. Le dashboard ne lit alors aucun rapport H1 et n'expose ni PnL, ni
fill, ni classement, ni agrégat, ni métadonnée dérivée du holdout. La route de
téléchargement répond seulement `REPORT_NOT_AVAILABLE`.

La variante primaire et toutes les variantes enregistrées restent visibles ;
aucune route ne permet de les reclasser. Le statut
`ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE` reste permanent tant qu'un contrat final
valide ne publie pas explicitement autre chose.

## Frontière Umbrel et réseau

La surface conserve l'architecture FastAPI existante, sans framework distant,
WebSocket de contrôle ou ressource Internet. Le service local est bindé sur
`127.0.0.1`; dans Umbrel, il reste accessible par le proxy interne. Les règles
non-root, rootfs read-only, `cap_drop: ALL`, `no-new-privileges`, absence de
Docker socket et absence de secret demeurent inchangées.
