# MESURES - TP1 Séance 1

## Prérequis
| Outil | Version relevée |
|---|---|
| Docker Engine | 29.5.3 |
| Docker Compose | v5.1.4 |
| Python | 3.14.0 |

## Question 1.1
Non, c2 ne voit pas le fichier (« fichier absent »). Chaque conteneur a sa propre couche inscriptible, l'image de base étant en lecture seule.
## Mesure 1 - Image et couches
| Grandeur | Valeur relevée |
|---|---|
| Taille de python:3.12-slim | 179 MB |
| Taille de l'image finale (vitals-producer:0.1) | 193 MB |
| Nombre de couches non vides dans docker history | 9 |
| Couche la plus volumineuse | base Debian (debian.sh trixie), 87.7 MB |

## Mesure 2 - Cache de build
| Scénario de rebuild | Durée | Étapes CACHED |
|---|---|---|
| Aucune modification | 0,6 s | 5 |
| Modification de producer.py | 1,3 s | 3 |
| Modification de requirements.txt | 5,8 s | 1 |

## Question 1.2
Le scénario 2 (modification de producer.py) changerait : le pip install ne serait plus en cache et serait refait à chaque modif du code, donc le rebuild serait beaucoup plus lent. Les scénarios 1 et 3 resteraient identiques.

## Décisions partie 2
| Décision | Valeur retenue |
|---|---|
| Nom du volume des journaux | vitals-logs (réel : s1-vitals-stream_vitals-logs) |
| Nom du réseau applicatif | vitals-net (réel : s1-vitals-stream_vitals-net) |
| Valeur de VITALS_TOPIC | mimic-vitals |

## Question 2.1
1. Un volume nommé est géré par Docker : il ne dépend pas d'un chemin de ma machine, donc le même compose marche sur n'importe quel poste (Windows, Mac, Linux).
2. Il évite les problèmes de droits et de format de fichiers entre l'hôte et le conteneur (notre conteneur tourne avec l'utilisateur 10001, un bind mount Windows poserait des soucis de permissions).

## Mesure 3 - Healthcheck
Délai observé avant healthy = 5,2 s (démarrage 13:44:49.368, premier test réussi 13:44:54.598).

| Clé du healthcheck | Valeur retenue | Justification |
|---|---|---|
| interval | 10s | assez court pour détecter un problème vite, sans charger le conteneur pour rien |
| retries | 3 | tolère un échec ponctuel, mais un service mort est détecté en 30 s environ |

## Décisions partie 3 (Kafka)
| Décision | Valeur retenue |
|---|---|
| Port hôte publié en face de 29092 | 29092 |
| KAFKA_PROCESS_ROLES | broker,controller |
| Facteur de réplication des topics internes | 1 |
| KAFKA_AUTO_CREATE_TOPICS_ENABLE | false |
| Nom du volume des journaux Kafka | vitals-kafka-data |

## Mesure 4 - Démarrage du broker
Temps entre up -d kafka et healthy : 7,3 s.

## Question 3.1
Impossible avec --replication-factor 2 : chaque réplique doit être sur un broker distinct, et il n'y a qu'un seul broker. Kafka renvoie InvalidReplicationFactorException (« The target replication factor of 2 cannot be reached because only 1 broker(s) are registered »).

## Mesure 5 - Répartition des clés (topic mimic-vitals, 3 partitions)
| Clé | Partition(s) et offsets observés |
|---|---|
| P001 | partition 2, offsets 0, 1, 2 |
| P002 | partition 1, offset 0 |
| P003 | partition 0, offset 0 |

## Question 3.2
Oui, les trois messages de P001 sont dans la même partition (2), avec des offsets consécutifs (0, 1, 2). Kafka ne garantit l'ordre qu'à l'intérieur d'une partition, et une même clé va toujours dans la même partition, donc l'ordre des mesures d'un patient est préservé. Entre patients (partitions différentes), aucun ordre n'est garanti.

## Mesure 6 - depends_on
| Forme de depends_on | Erreurs de connexion au démarrage ? | Lignes d'erreur avant stabilisation |
|---|---|---|
| depends_on: [kafka] | Oui (Connection refused) | 3 (2 refus de connexion + 1 avertissement d'idempotence) |
| condition: service_healthy | Non | 1 (avertissement d'idempotence seulement, pas de refus de connexion) |

La forme courte attend seulement que le conteneur Kafka soit lancé (0,6 s), alors que service_healthy attend que le broker réponde (13,6 s).

## Mesure 7 - Débit (INTERVAL_S=1.0, NB_PATIENTS=12)
| Grandeur | Valeur |
|---|---|
| Somme des offsets de fin avant (t0) | 7913 |
| Somme des offsets de fin après 60 s (t1) | 8645 |
| Débit calculé (t1 - t0) / 60 | 12,2 msg/s |
| Débit annoncé par le producteur à l'arrêt | 12,0 msg/s |
| INTERVAL_S et NB_PATIENTS utilisés | 1.0 et 12 |

## Question 5.1
Oui : NB_PATIENTS / INTERVAL_S = 12 / 1,0 = 12 msg/s, et j'ai mesuré 12,2 msg/s, soit un écart de 1,7 % (sous les 5 %). Les 732 messages correspondent à 61 tours de 12 patients : ma fenêtre a duré environ 61 s, car les deux appels docker compose exec prennent un peu de temps.

| Grandeur (INTERVAL_S=0.1) | Valeur |
|---|---|
| Débit mesuré | 121,6 msg/s |
| Écart avec la valeur théorique (120 msg/s) | +1,3 % |

## Mesure 8 - Coupure du broker pendant la production
| Observation | Valeur relevée |
|---|---|
| Le processus producteur s'arrête-t-il ? | Non, il reste Up (healthy) et continue de tourner |
| Message d'erreur exact (première ligne) | kafka:9092/1: Disconnected (after 15433ms in state UP) |
| Délai entre stop et la première erreur | 0,3 s (stop à 14:20:53.759, erreur à 14:20:54.072) |
| Après start, la production reprend-elle sans relancer le script ? | Oui, plus d'erreur après la dernière à 14:22:26 (broker en démarrage) |
| Des messages produits pendant la coupure ont-ils été perdus ? | Non : bilan envoyes=1656 non_livres=0 |

## Question 5.2
Entre stop et start, le producteur ne plante pas : la bibliothèque tente de se reconnecter en boucle (erreurs Disconnected, Connection refused, Failed to resolve) pendant que mon code continue d'appeler produce(). Les messages s'accumulent dans la file d'attente interne du producteur et sont conservés tant qu'ils n'ont pas expiré. Avec acks=all et enable.idempotence=True, un message n'est considéré comme livré que quand le broker confirme, et un renvoi ne crée ni doublon ni inversion d'ordre : au retour du broker, tout est envoyé dans l'ordre. La durée de tenue est limitée par le délai maximal de livraison d'un message (message.timeout.ms, 300 s par défaut dans librdkafka) et par la taille de la file (100 000 messages par défaut), au-delà desquels les messages sont abandonnés et accuse_reception reçoit une erreur.

## Question 6.1
down supprime les conteneurs et le réseau mais conserve les volumes nommés : les données du broker (topic, messages, offsets) survivent, et au prochain up le topic mimic-vitals est toujours là. down -v supprime aussi les volumes, donc toutes les données de Kafka : au prochain up le broker est vide et, la création automatique de topics étant désactivée, il faut recréer mimic-vitals avant de produire.

## Décisions partie 4 (producteur)
| Paramètre | Valeur retenue | Justification |
|---|---|---|
| acks | all | le broker confirme l'écriture complète, le plus sûr (requis par l'idempotence) |
| linger.ms | 50 | regroupe les messages en lots : plus de débit pour 50 ms de latence |
| partitioner | murmur2_random | seule valeur qui place une clé dans la même partition que le client Java (Flink plus tard) |
| INTERVAL_S | 1.0 | un moniteur ICU échantillonne environ 1 fois par seconde |

## Note sur Python
Python 3.14.0 est la version par défaut du poste. confluent-kafka 2.6.1 n'a pas de wheel pour la 3.14 (erreur de compilation, compilateur C++ absent), donc le test depuis l'hôte a été fait avec Python 3.11.1 dans un venv. Le conteneur utilise Python 3.12 (python:3.12-slim).
