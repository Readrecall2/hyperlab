# Phase 14 — revue sécurité et préparation micro-mainnet

Ne coder le mainnet qu'après revue séparée du testnet et décision humaine.

## Audit

- threat model ;
- dépendances et SBOM ;
- secret scanning ;
- permissions conteneur ;
- séparation collector/dashboard/executor ;
- fail-closed réseau et environnement ;
- limites impossibles à dépasser depuis la stratégie ;
- procédure de révocation ;
- sauvegarde et reprise ;
- journal append-only ;
- tests chaos.

## Double verrou futur

Le mainnet doit exiger deux confirmations indépendantes, une configuration signée et une limite de capital codée. Le défaut est toujours `paper` ou `testnet`.

## Micro-mainnet

100–300 USDC, 1×, une seule stratégie, validation humaine, aucune montée automatique. Le but est de calibrer fills, slippage et frais, pas de maximiser le rendement.
