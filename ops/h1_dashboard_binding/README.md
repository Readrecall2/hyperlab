# Generic H1 Dashboard Binding

Ce répertoire contient seulement le mécanisme réutilisable et fail-closed de
liaison d'un dashboard H1 à une campagne. Il ne fige aucune identité de campagne
et ne fournit donc aucun bundle directement exécutable. Statut :
`H1_DASHBOARD_GENERIC_INTEGRATION_READY_AWAITING_V8_IDENTITY`.

Le validateur exige un plan JSON externe complet avant tout rendu. Le plan lie
explicitement les quatre commits distincts (base de lancement, dashboard
original, cherry-pick d'intégration et source finale), la branche, l'identité et
le hash du manifest, les chemins exacts, le service collecteur et le service
dashboard séparé. `manifest_checks` contient uniquement les champs réellement
présents dans le manifest canonique ; le slug, le chemin et le commit collecteur
sont liés ailleurs dans le plan sans inventer de champs de manifest.

Les invariants non paramétriques sont :

- `PAPER_ONLY/GHOST_ONLY/PUBLIC_DATA_ONLY`, API GET/HEAD et mode read-only ;
- écoute IPv4 exclusivement sur `127.0.0.1`, port non privilégié ;
- source dashboard dédiée puis root-owned sans bit d'écriture ;
- campagne montée par self-bind systemd en lecture seule ;
- `ProtectSystem=strict`, `NoNewPrivileges`, capacités vides, secrets bannis ;
- collecteur inspecté uniquement avec `systemctl show`, jamais muté ;
- accès Beelink via `ssh -N -T -L 127.0.0.1:PORT:127.0.0.1:PORT` avec
  `ClearAllForwardings=yes` et `ExitOnForwardFailure=yes`.

## Séquence future, après gel humain de l'identité V8

1. Windows PowerShell : valider le plan, produire et transférer un bundle depuis
   un worktree propre. Durée attendue 2–10 min, maximum 30 min ; seuls la clé SSH
   ou le host-key peuvent demander confirmation ; Ctrl+C stoppe le transfert ;
   signal attendu : `H1_DASHBOARD_BINDING_TRANSFER_GREEN_NOT_INSTALLED`.
2. Tabby/Bash VPS : installer la source dashboard séparée et l'unité rendue.
   Durée attendue 10–25 min, maximum 45 min ; `sudo` peut demander le mot de
   passe ; monitorer dans un second onglet avec `systemctl show` et des GET
   loopback ; Ctrl+C avant enable stoppe l'installation, après enable le service
   reste géré par systemd ; signal : `H1_DASHBOARD_BINDING_INSTALL_GREEN`.
3. Windows PowerShell : exécuter le tunnel rendu. Session foreground, durée
   contrôlée par l'opérateur ; Ctrl+C ferme seulement le tunnel ; le signal de
   succès est un GET navigateur sur l'URL loopback.

Le bootstrap Linux vérifie explicitement
`python -m hyperlab h1-dashboard-serve --help`. L'unité rendue utilise le fichier
réel `config/research.toml`, passe le commit source final au dashboard et conserve
séparément les commits dashboard original et d'intégration.

La preuve causale finale `integration -> base -> source`, le nom de branche et
l'identité de campagne restent un gate différé : ils ne pourront être gelés et
validés qu'au jalon V8, lorsque ces valeurs existeront réellement.
