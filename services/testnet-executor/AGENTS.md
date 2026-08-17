# AGENTS.md — politique HyperLab Testnet Executor 0.3.x-dev

## Portée

Cette politique s'applique uniquement à `services/testnet-executor/`. Le paquet
racine HyperLab 0.2.x, le collecteur, le dashboard, Umbrel et `src/hyperlab/paper/`
restent read-only vis-à-vis des venues et ne reçoivent jamais de secret ni de
transport d'ordre.

## Autorisation limitée

- L'environnement exact est `TESTNET` et le but exact `TESTNET_EXECUTION`.
- Seuls `https://api.hyperliquid-testnet.xyz` et
  `wss://api.hyperliquid-testnet.xyz/ws` sont autorisés.
- Aucun endpoint par défaut, fallback, remplacement d'hôte, proxy applicatif ou
  sélection dynamique de réseau n'est permis.
- Le service peut signer uniquement les actions L1 nécessaires aux ordres,
  annulations, modifications et dead-man switch Hyperliquid Testnet.
- Ne jamais importer ni instancier `hyperliquid.exchange.Exchange` : sa valeur par
  défaut Mainnet est incompatible avec la frontière fail-closed. Utiliser les
  primitives de signature auditées avec un transport HTTP Testnet explicitement
  construit.
- Aucun transfert, retrait, bridge, délégation, approbation d'agent, builder fee,
  staking, vault ou action non liée au cycle d'ordre n'est autorisé.

## Secrets

- Utiliser exclusivement le namespace `HYPERLAB_TESTNET_*` documenté.
- Ne jamais lire un secret générique, Paper, Micro-Mainnet ou Mainnet.
- Ne jamais écrire de clé, seed, mnemonic, signature ou payload signé dans Git,
  `.env`, SQLite, les logs, les erreurs, les tests, les rapports ou le dashboard.
- Les objets qui contiennent un secret doivent avoir une représentation redacted
  et ne doivent jamais être sérialisables.

## Cycle d'ordre et reprise

- Persister intention, CLOID déterministe et tentative ambiguë avant tout I/O.
- Un timeout, une connexion coupée ou un crash après préparation impose une
  recherche/réconciliation par CLOID ; aucun renvoi aveugle.
- Les fills sont dédupliqués par identité venue stable et comptés une seule fois.
- Une divergence ordre/position/fill/solde non résolue place durablement le runtime
  en `PAUSED` / `MANUAL_REVIEW`.
- Les ordres ambigus et cancel-pending réservent leur exposition au pire cas.
- Pause et kill sont durables et bloquent toute nouvelle intention avant l'appel
  réseau éventuel.

## Tests et validation

- Aucun test automatisé ne contacte un réseau réel, Testnet ou Mainnet.
- Utiliser uniquement des transports, horloges et credentials synthétiques.
- Toute fixture synthétique porte une mention visible et n'est jamais une preuve
  de Gate E, de rentabilité ou de liquidité.
- Tester les fenêtres de crash avant/après submit, acknowledgement, fill, cancel
  et replace, ainsi que redelivery, perte de réponse et reconnexion.
