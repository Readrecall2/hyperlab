# Politique de risque

## Recherche

Chaque stratégie possède un profil de limites séparé. Le moteur limite le poids par instrument, l'exposition brute et l'exposition nette. Les limites ne servent pas à brider le rendement ; elles définissent la stratégie réellement déployable.

## Paper/testnet futurs

- machine à états persistante ;
- identifiants d'ordres déterministes ;
- traitement idempotent ;
- réconciliation exchange-first au redémarrage ;
- données périmées = aucune entrée ;
- jambe non couverte = hedge ou débouclage immédiat ;
- `REDUCE_ONLY`, `PAUSED`, `MANUAL_REVIEW` et `EMERGENCY_FLATTEN` ;
- dead-man switch renouvelé ;
- limites de perte quotidienne et drawdown.

## Mainnet futur

- sous-compte réservé au bot ;
- API wallet dédiée et révocable ;
- aucune seed principale sur le serveur ;
- montant initial insignifiant ;
- aucune augmentation automatique ;
- confirmation humaine des premiers cycles ;
- clé accessible uniquement au conteneur exécuteur, jamais au dashboard/collecteur ;
- retrait manuel depuis le wallet principal.

## Umbrel

L'Umbrel actuel convient à la collecte, au dashboard et au paper lent. Il ne doit pas contenir une clé importante s'il héberge déjà un nœud Bitcoin et d'autres services. Une production sérieuse justifiera une machine dédiée ou un VPS minimal, surtout pour les stratégies sensibles à la latence.
