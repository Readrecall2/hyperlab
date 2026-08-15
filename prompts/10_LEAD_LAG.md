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

Le seuil d'incertitude d'horloge reste strictement 50 ms. Toute observation au-
dessus est persistée `INVALID`, avec son RTT et sa raison, et ne peut jamais
servir de preuve valide. Au plus une rejection haute-RTT consécutive peut être
franchie si les observations acceptées conservent une couverture causale continue
dans la même génération ; une observation valide remet la série à zéro. La
deuxième probe rejetée consécutive révoque la couverture depuis son temps de
réponse, même si l'intervalle valide antérieur de 15 secondes n'a pas expiré.
Cette outage reste ouverte jusqu'à la prochaine observation acceptée de la même
génération ; la récupération peut rendre les données marché ultérieures
utilisables, jamais la période révoquée.

La cadence d'acquisition est contrôlée séparément sur tous les
`request_sent_time` des lignes `clock_sync` v2 exactement liées à la
génération publique, qu'elles soient `VALID` ou `INVALID`. Une ligne
rejetée compte comme tentative persistée, jamais comme preuve causale. L'écart
entre deux lancements ne peut dépasser 10 000 ms, sans epsilon. La couverture et
les bandes d'offset restent dérivées exclusivement des observations acceptées ;
une violation de cadence ne fabrique pas un intervalle causal invalide. La borne
de 10 secondes couvre aussi activation vers première tentative et dernière
tentative vers événement terminal lié ou fin de fenêtre. Un échec de requête ou
une identité non liée ne peut pas créditer la cadence.

Le gate échoue si l'assessment causal intersecte cette outage. Une outage
pré-fenêtre récupérée avant l'assessment reste rapportée sans condamner les
données postérieures. Une absence de récupération, un trou après une seule
rejection, une observation acceptée trop âgée, une tentative manquante au-delà
de 10 secondes, une discontinuité d'offset, un échec de requête, une identité ou
policy invalide, ou
une génération active sans mesure valide reste fatal. Il est interdit
d'interpoler ou de promouvoir une mesure rejetée.

La collecte doit aussi démontrer un débit writer durable confortablement
supérieur au pic Binance avec résidence de file bornée. Il est interdit de faire
passer ce gate en augmentant les capacités 10 000/20 000, en relâchant une
barrière de durabilité ou en masquant une saturation. Les frames logiques restent
atomiques à l'admission ; Parquet, manifestes, ordre, lignée et récupération
après crash restent immuables et fail-closed.

Le baseline horaire inclus doit être remplacé par un replay event-driven. Sans cela, aucune conclusion de rentabilité n'est admise.
