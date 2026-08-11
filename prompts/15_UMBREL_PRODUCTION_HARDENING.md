# Phase 15 — durcissement Umbrel et déploiement

## Objectif

Transformer le package read-only en service de collecte fiable, sans lui ajouter de clé.

## Travaux

- healthchecks ;
- limites CPU/RAM/disque ;
- rotation des logs ;
- sauvegarde et export Parquet ;
- bouton de téléchargement des rapports ;
- alerte de données stale ;
- mise à jour versionnée et rollback ;
- image multi-architecture signée ;
- SBOM et scan de vulnérabilités ;
- documentation de désinstallation et conservation des données.

## Règle

Même après durcissement, Umbrel ne reçoit pas automatiquement l'exécuteur mainnet. Toute clé importante reste sur une machine dédiée avec surface minimale.
