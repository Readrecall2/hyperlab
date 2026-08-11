# Phase 03 — venues de référence publiques

## Objectif

Ajouter au moins une venue de référence liquide pour le funding cross-exchange, le lead-lag et la fair value du market making.

## Travaux

- choisir une API publique documentée, sans clé pour les données ;
- collecter BBO, trades, funding et candles ;
- normaliser symboles et tailles ;
- conserver timestamps source et réception ;
- mesurer le clock drift et la latence ;
- isoler chaque connecteur derrière une interface commune ;
- ajouter un replay multi-venue synchronisé ;
- documenter les limites et conditions d'utilisation.

## Contrôles

- ne jamais appeler une API de trading ;
- ne jamais supposer que deux contrats ont le même mark/oracle ;
- détecter les périodes où une venue est absente ou en maintenance ;
- tests de désynchronisation et messages hors ordre.
