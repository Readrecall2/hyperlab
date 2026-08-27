# H1 V8 Dashboard Binding V1

Ce pack lie le cockpit H1 read-only à la campagne V8
`h1-20260827t004500z-5973abde`. Il ne modifie, ne redémarre et ne remplace
jamais le collecteur existant. Il ne contient aucune route d'ordre, aucun
wallet, signer, secret, endpoint privé ou accès real-money.

L'input suivi `binding-input-v8.json` fige la base collecteur, le cherry-pick du
dashboard et son commit original, l'identité campagne/manifest, les chemins VPS,
le service dashboard séparé et `127.0.0.1:18080`. Il omet volontairement le
commit source final. Après le commit final, le générateur PowerShell l'injecte
dans `binding-plan.json` et vérifie :

- HEAD, branche historique exacte et worktree propre ;
- parent direct `base -> integration`, puis ancestry `integration -> source` ;
- marqueur exact du cherry-pick vers le commit dashboard original ;
- Git bundle exposant uniquement la ref attendue au commit final ;
- hashes des fichiers suivis, du bundle et des artefacts essentiels transmis.

`binding-files.sha256` couvre l'ensemble essentiel non circulaire : bundle,
input, plan, handoff, README, bootstrap et unité. Son propre SHA-256 est injecté
dans les étapes 01 et 02 ; chacune vérifie ce pin avant de parser la liste ou
d'appeler `sha256sum -c`. Les blocs opérateur sont produits seulement après ce
gel, ce qui évite toute auto-référence.

L'unité générée écoute uniquement sur IPv4 loopback, monte le campaign root par
self-bind en lecture seule, rend la source et le handoff immuables, utilise
`ProtectSystem=strict`, `ProtectHome=yes`, `NoNewPrivileges`, aucune capacité,
aucun fichier de secrets et aucune écriture dans la campagne. Son préflight
vérifie le manifest canonique, les checkouts source, le port et le collecteur via
la seule commande `systemctl show`.

La preuve runtime opérateur acquise le `2026-08-27T00:45:31Z` est
`H1_SERVICE_RUNNING_HEALTH_GREEN` : collecteur active/running, PID principal
`118072`, `NRestarts=0`, `ExecMainStatus=0`, santé terminale `RUNNING`. Après
21,106 s, la santé publiée comptait 834 frames, 0 gap, 0 reconnect, un
`queue_high_water` de 9, 0 segment et 0 octet stocké, sans erreur. Ces valeurs
sont une observation historique, pas un pin de disponibilité : le cockpit
représente honnêtement les zéros initiaux et admet un manifest raw encore nul
pendant `RUNNING_HEALTHY` sans ouvrir le holdout ni inventer une erreur.

## 00 — Finalisation locale après le commit final

- lieu : Windows PowerShell, dans le worktree isolé du Beelink ;
- durée attendue : 1–3 minutes ; maximum : 10 minutes ;
- prompts : aucun ;
- monitoring : validation input, Git bundle, SHA-256 et inventaire final ;
- Ctrl+C : interrompt uniquement la génération locale, sans action VPS ;
- signal terminal :
  `H1_V8_DASHBOARD_BINDING_WINDOWS_BUNDLE_FINALIZED_NOT_TRANSFERRED`.

```powershell
& '.\ops\h1_dashboard_binding\New-H1V8DashboardBindingBundle.ps1' `
  -Commit '<COMMIT_FINAL_40_HEX>' `
  -OutputRoot 'C:\hyperlab-offline-validation\h1-v8-dashboard-binding-v1'
```

Le répertoire de sortie doit être nouveau. Le générateur refuse toute branche,
source, causalité ou propreté divergente.

## 01–03 — Exécution humaine ordonnée

Exécuter ensuite, sans les fusionner :

1. `operator/01-windows-transfer.ps1.txt` dans Windows PowerShell sur le
   Beelink. Le signal terminal confirme uniquement le transfert, pas
   l'installation.
2. `operator/02-tabby-vps-install.sh.txt` dans Tabby/Bash sur le VPS. Ce bloc
   clone le bundle vers une source dashboard dédiée, crée le venv hash-locké,
   compare le rendu canonique de l'unité, rend source et plan root-owned/read-only,
   installe et démarre uniquement le service dashboard, puis vérifie GET, HEAD et
   l'unique listener `127.0.0.1:18080`.
3. `operator/03-windows-tunnel.ps1.txt` dans Windows PowerShell uniquement après
   le signal vert Tabby. Le tunnel foreground est strictement
   `ssh -N -T -L 127.0.0.1:18080:127.0.0.1:18080` avec
   `ClearAllForwardings=yes` et `ExitOnForwardFailure=yes`. Ctrl+C ferme seulement
   ce tunnel.

Chaque bloc contient son lieu, ses durées attendue/maximale, ses prompts, son
monitoring, l'effet de Ctrl+C et son signal terminal. Aucun bloc n'ouvre de
firewall. Le second onglet Tabby proposé est purement read-only ; le dashboard
suit `PREPARED_NOT_STARTED -> RUNNING_HEALTHY` sans relance ni mutation de la
campagne. Le holdout demeure scellé.

Verdict du pack :
`H1_V8_DASHBOARD_BINDING_V1_GREEN_AWAITING_HUMAN_VPS_EXECUTION`.
