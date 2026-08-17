# Phase 14 — revue sécurité et préparation micro-mainnet

Ne coder aucune route Mainnet avant un service Testnet séparé validé, Gates
B/C/D/E satisfaites et une décision humaine. HyperLab 0.2.x peut évaluer cette
politique, mais reste incapable d'émettre ou consommer un reçu d'exécution réelle.

## Audit

- threat model, dépendances, SBOM et secret scanning ;
- séparation collector/dashboard/executor et permissions minimales ;
- identité d'environnement et endpoint fail-closed, sans fallback ;
- signer isolé, secrets dédiés/révocables et procédure de révocation ;
- limites notionnel/position/perte/drawdown impossibles à dépasser depuis la stratégie ;
- réconciliation, sauvegarde/reprise, journal append-only, kill switch et tests chaos.

## Autorisation `MICRO_MAINNET`

Un reçu exact `MICRO_MAINNET` / `MICRO_MAINNET_EXECUTION` exige Gates B/C/D et
la preuve Gate E terminée, toutes reliées avec vérification octet par octet à une
même lignée figée de stratégie, modèle économique et risque. Les configurations et reçus Testnet et
micro-mainnet restent distincts : le reçu Testnet n'est jamais consommé ici. Il
exige aussi une configuration signée, un signer isolé, une revue humaine et les
contrôles opérationnels ci-dessus. Limites initiales : 100–300 USDC,
levier 1×, une stratégie, une paire/position et aucune montée automatique. Le but
est de calibrer fills, slippage et frais, pas de maximiser le rendement.

Gate F est la preuve de la campagne micro-mainnet terminée, liée à sa configuration
exacte. Elle est requise avant Mainnet et n'autorise aucune hausse automatique.

## `MAINNET`

`MAINNET` exige un reçu séparé exact `MAINNET` / `MAINNET_EXECUTION`, la preuve
Gate F et deux confirmations humaines indépendantes. Un reçu Research, Paper, Testnet
ou micro-mainnet ne peut jamais être converti ou réutilisé. Il n'existe aucun
environnement d'exécution par défaut : une identité manquante ou ambiguë bloque le
démarrage.
