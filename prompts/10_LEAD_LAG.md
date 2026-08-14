# Phase 10 — lead-lag sub-seconde

## Objectif

Tester proprement si une venue de référence apporte une information exploitable avant Hyperliquid.

## Préconditions

Ne commencer que lorsque les flux multi-venues sub-seconde sont continus, horodatés et rejouables.

## Études

- cross-correlation par lag ;
- réponse impulsionnelle ;
- classification des mouvements informatifs vs bruit ;
- modèle simple avant ML complexe ;
- latence de décision, réseau, ordre et fill ;
- seuil d'entrée après tous coûts ;
- capacité et concurrence ;
- stabilité par heure, actif et régime.

## Simulation

- prix exécutable, pas mid futur ;
- délai réel mesuré ;
- rejets et ordres ratés ;
- variation pendant le trajet ;
- fill partiel ;
- sortie adverse.

## Gate

Statut actuel : `BLOCKED_PRECONDITION_NOT_MET`. Ne lancer aucune étude
économique tant qu'une nouvelle collecte réelle BTC/ETH n'a pas démontré des
trades Binance non nuls, une couverture `clock_sync` causale continue et un
chevauchement strict BBO + L2 + trades des deux venues supérieur à zéro.

Le baseline horaire inclus doit être remplacé par un replay event-driven. Sans cela, aucune conclusion de rentabilité n'est admise.
