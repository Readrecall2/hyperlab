# H1 V8 Dashboard Binding V2 — correction CRLF et parent dédié

Ce pack lie le cockpit read-only à la campagne V8
`h1-20260827t004500z-5973abde` sans modifier son collecteur, ses artefacts ou
son holdout. Il corrige causalement les deux refus V1 observés : scripts
matérialisés en CRLF et création implicite de `dashboard-sources` par
`install -d` sous `umask 077`.

La preuve V1 figée comptait 169 CRLF dans le script d'installation : SHA-256
original `c39229842aaa66c831d32a3cef8a5bad7445489d4f7e16f587cf448ee3308f53`,
SHA-256 du flux LF authentifié
`17d7c37f6166c4da6043a5acc0c962b4c2e68b9791193c14bdd743c9a5c1a5ec`.
Le second refus a établi le parent dédié `root:root:0700` et un leaf V1
inaccessible, sans clone, handoff, unité ou dashboard installé.

Le pack V1, son incoming et son leaf partiel restent conservés. V2 utilise des
identités neuves :

- incoming : `/home/hyperlab/hyperlab-h1/dashboard-bindings/h1-20260827t004500z-5973abde-dashboard-v2` ;
- source : `/mnt/HC_Volume_106716684/hyperlab-h1/dashboard-sources/h1-20260827t004500z-5973abde-dashboard-v2` ;
- handoff : `/etc/hyperlab-h1-dashboard/h1-20260827t004500z-5973abde-dashboard-v2` ;
- service : `hyperlab-h1-dashboard-20260827t004500z-5973abde-v2.service`.

Tous les scripts exécutables ont une extension native `.ps1` ou `.sh` et des
octets UTF-8/LF sans BOM, NUL ou CR. Les trois scripts opérateur sont inclus
dans `binding-files.sha256`. Pour éviter une identité circulaire, le SHA-256 de
cet inventaire est un argument obligatoire de A et B ; il est publié par le
générateur après matérialisation complète.

## Frontière opérationnelle

Le dashboard écoute uniquement `127.0.0.1:18080`, répond uniquement à GET/HEAD,
publie `mode=readonly` et `orders_enabled=false`, et n'est accessible depuis le
navigateur du Beelink que par le tunnel SSH local C. Aucun firewall n'est
ouvert. L'unité utilise notamment `ProtectSystem=strict`, `NoNewPrivileges=yes`,
un jeu de capacités vide, aucun secret et un bind read-only de la campagne.

Le script B ne contient pour le collecteur que `systemctl show` et des lectures
d'artefacts/checkouts. Il ne lance aucune action start/restart/stop/enable,
disable ou recovery sur H1. Avant toute création, il refuse si le port 18080,
le service V2, le handoff V2 ou le leaf V2 existent déjà.

Le parent `dashboard-sources` est créé séparément du leaf. S'il existe en
`root:root:0700`, B ne corrige que ce parent avec un `chown --no-dereference`
non récursif, et seulement si son unique contenu est le résidu V1 exact,
répertoire réel `hyperlab:hyperlab:0700`, vide, canonique et sur le device
attendu. Les liens, contenus étrangers, modes, propriétaires ou devices
divergents sont refusés. Le leaf V1 est inspecté sans suivre de lien et n'est
jamais supprimé, réutilisé ou modifié.

## État H1 acquis, non requalifié

L'observation autoritaire fournie avant V2 est `INTERRUPTED_RECOVERABLE` :
3 999 005 frames, 358 segments, 1 092 828 859 octets, 0 gap et 19 reconnects.
Le collecteur est active/running mais totalise 66 redémarrages. Ce n'est pas une
preuve `RUNNING_HEALTHY` et ce pack n'effectue aucune recovery. Le cockpit admet
et affiche honnêtement `INTERRUPTED_RECOVERABLE`, les compteurs y compris zéro,
et un hash raw/final optionnel nul sans fabriquer d'erreur. Le holdout demeure
scellé.

## Génération locale après le commit final

- lieu : Windows PowerShell 5.1, worktree isolé Beelink ;
- durée attendue : 1–3 minutes, maximum 10 minutes ;
- prompts : aucun ;
- monitoring : bundle Git, SHA-256, inventaire et verdict final ;
- Ctrl+C : interrompt uniquement la génération locale ;
- signal : `H1_V8_DASHBOARD_BINDING_V2_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED`.

```powershell
& '.\ops\h1_dashboard_binding\New-H1V8DashboardBindingV2Bundle.ps1' `
  -Commit '<COMMIT_FINAL_40_HEX>' `
  -OutputRoot 'C:\hyperlab-offline-validation\h1-v8-dashboard-binding-v2'
```

Le répertoire de sortie doit être nouveau. Le générateur refuse une branche,
un HEAD, un ref, une causalité ou une propreté divergents.

## A, B, C — exécution humaine strictement ordonnée

1. Dans Windows PowerShell sur le Beelink, exécuter le fichier natif
   `operator/A-windows-transfer.ps1` depuis n'importe quel cwd avec
   `-ExpectedInventorySha256 <SHA256_INVENTAIRE>`. Son signal vert signifie
   seulement transfert terminé ; le dashboard n'est pas installé.
2. Dans Tabby/Bash sur le VPS, exécuter
   `bash ./operator/B-tabby-vps-install.sh <SHA256_INVENTAIRE>`. Le signal vert
   confirme le service V2 séparé, le listener loopback et les contrats GET/HEAD.
3. Après ce signal uniquement, dans Windows PowerShell sur le Beelink, exécuter
   `operator/C-windows-tunnel.ps1`. Le processus SSH reste au premier plan ;
   Ctrl+C ferme uniquement le tunnel.

Chaque script contient son lieu, sa durée attendue/maximale, ses prompts, son
monitoring, l'effet de Ctrl+C et son signal terminal. Le monitoring continu
proposé dans un second onglet Tabby est read-only.

Verdict local attendu :
`H1_V8_DASHBOARD_BINDING_V2_GREEN_CRLF_PARENT_OWNERSHIP_FIXED_PUSHED_AWAITING_HUMAN_TRANSFER`.
