# Mesures — séance 2

**Ingestion et persistance : consommateur, consumer groups et PostgreSQL**
Projet HealthStream ICU Monitor · topic `mimic-vitals` · groupe `vitals-persister` · table `vitals_raw`

---

## Environnement et adaptations

| Élément | Valeur |
|---|---|
| Poste | Windows + WSL Ubuntu 22.04 |
| Docker | Docker Engine 28.2.2 installé dans WSL (pas Docker Desktop) |
| Python hôte | 3.10.12 (venv `.venv`) ; les conteneurs utilisent Python 3.12 |
| `docker compose config --quiet` | aucune sortie (compose OK) |
| Volume Kafka existant au départ | non : moteur Docker neuf, topic recréé à l'identique de la S1 (3 partitions) |
| PostgreSQL natif sur le poste | aucun |

Adaptations par rapport à l'énoncé :

- **Image Kafka de la S1 = `confluentinc/cp-kafka:7.8.0`** (et non l'image Apache) : les outils s'appellent sans `/opt/kafka/bin/` ni `.sh` (`kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets`, `kafka-console-consumer`), et le console consumer prend `--property print.key=true` au lieu de `--formatter-property`. Le volume Kafka s'appelle `vitals-kafka-data` (nom fixé en S1).
- **Dockerfile du consommateur** : même patron que le producteur de la S1 (`python:3.12-slim`, `PYTHONDONTWRITEBYTECODE=1`, `PYTHONUNBUFFERED=1`, `requirements.txt` installé avant le code, utilisateur `appuser` UID 10001). L'énoncé cite `wolfi-base`, mais le producteur S1 est basé sur `python:3.12-slim` : on reste cohérent avec lui.
- Plusieurs expériences ont été laissées tourner plus longtemps que la durée indiquée ; c'est précisé à chaque fois, et les calculs en tiennent compte.

---

## Partie 1 — Reprise de l'artefact S1 et passage à plusieurs partitions

### Mesure 1 — état de départ

| Grandeur | Valeur |
|---|---|
| Nombre de partitions de `mimic-vitals` avant modification | 3 |
| Somme des offsets de fin | 0 (topic neuf) |

Débit du producteur relevé pendant la partie 1 : 276 messages en 23,1 s (12,0 msg/s), puis 432 en 33,5 s (12,9 msg/s) → **~12,5 msg/s**.

### Mesure 2 — répartition avant modification (3 partitions)

| Partition | Patients observés | Nombre |
|---|---|---|
| 0 | P003, P005, P008, P009, P010, P012 | 6 |
| 1 | P002, P006, P007, P011 | 4 |
| 2 | P001, P004 | 2 |

### Choix du nombre de partitions

`NB_PARTITIONS=6` : plus petit multiple commun de 2 et 3. Avec 2 consommateurs, 3 partitions chacun ; avec 3 consommateurs, 2 chacun. Aucun membre inactif, aucun membre avec plus de partitions qu'un autre.

Après `--alter` : `PartitionCount: 6`, partitions 0 à 5, toutes `Leader: 1`, `Isr: 1`.
Bornes relevées juste après la modification : p0=354, p1=236, p2=118, p3=0, p4=0, p5=0 (total 708). Le ratio 354/236/118 reproduit exactement le ratio 6/4/2 de patients (59 messages par patient) : le volume d'une partition dépend du nombre de clés qu'elle porte.

### Mesure 3 — répartition après modification (messages récents uniquement)

Run du producteur après `--alter` : 696 messages en 52,9 s (13,2 msg/s). Lecture de chaque partition à partir de sa borne.

| Partition | Patients observés | Nombre |
|---|---|---|
| 0 | P005, P010, P012 | 3 |
| 1 | P007 | 1 |
| 2 | aucun | 0 |
| 3 | P003, P008, P009 | 3 |
| 4 | P002, P006, P011 | 3 |
| 5 | P001, P004 | 2 |

**Q1.1** — Non : 7 patients sur 12 ont changé de partition (P003, P008, P009 de 0 vers 3 ; P002, P006, P011 de 1 vers 4 ; P001, P004 de 2 vers 5). La partition vaut hash(clé) modulo le nombre de partitions : passer le diviseur de 3 à 6 change le reste pour une partie des clés, et comme h mod 3 = (h mod 6) mod 3, une clé de l'ancienne partition p ne peut aller que vers p ou p+3, ce qui est exactement ce qu'on observe.

**Q1.2** — Non, la répartition n'est pas uniforme : 3/1/0/3/3/2, et la partition 2 ne reçoit plus aucun patient. Le hachage n'équilibre qu'en moyenne sur un grand nombre de clés ; avec 12 clés sur 6 partitions (moyenne 2, écart-type ≈ 1,3), des partitions à 0 ou 3 clés sont un résultat ordinaire. En partie 3, les consommateurs n'auront donc pas la même charge : celui qui détient la partition 2 lira peu ou rien, quelle que soit sa puissance.

**Q1.3** — La garantie cassée est l'ordre par clé : les anciens messages de P001 sont restés en partition 2, les nouveaux vont en partition 5, et un lecteur qui lit ces partitions indépendamment peut les voir dans le désordre. Cette fenêtre dure jusqu'à ce que la rétention du topic efface les anciens messages de la partition 2.

### 1.4 — Contrôle de fin de partie

`--describe` : 6 partitions, toutes `Leader: 1`.
Offsets de fin : p0=528, p1=294, p2=118, p3=174, p4=174, p5=116 → total 1404 (contre 0 à la mesure 1). La partition 2 reste figée à 118 : plus aucun patient n'y tombe depuis le passage à 6 partitions.
Producteur relancé en conteneur (`docker compose up -d --build producer`) : il alimente le topic pendant tout le TP.

---

## Partie 2 — Premier consommateur : offsets et acquittement

### Décisions de `lecteur.py`

- `auto.offset.reset` : lu depuis `AUTO_OFFSET_RESET`, défaut `earliest` (un groupe neuf part du plus ancien message disponible ; on ne veut rien rater).
- Ajout d'une variable `ASSIGNMENT_STRATEGY` (défaut `range`) pour passer en `cooperative-sticky` en 3.4 sans modifier le code.

### Mesure 4 — `auto.offset.reset` sur un groupe neuf

| Groupe | `auto.offset.reset` | 1er lancement | 2e lancement |
|---|---|---|---|
| explo-earliest | earliest | 7345 | 1944 |
| explo-latest | latest | 1069 | 1236 |

- explo-earliest, 1er lancement : relit tout l'historique depuis l'offset 0 de chaque partition.
- explo-earliest, 2e lancement : reprend à l'offset commité (partition 0 à partir de l'offset 2062) ; seuls les messages arrivés depuis sont lus.
- explo-latest, 1er lancement : démarre à la fin du journal, ne lit que les nouveaux messages (≈ 13 msg/s × 82 s).
- explo-latest, 2e lancement : reprend à l'offset commité au 1er lancement, `latest` ne s'applique plus.

Note : lancements laissés ~70 à 100 s au lieu de 20 s, ce qui gonfle les chiffres sans changer la conclusion.

### Mesure 5 — `--describe` sur explo-earliest (partition 0)

| Colonne | Valeur |
|---|---|
| CURRENT-OFFSET | 2499 |
| LOG-END-OFFSET | 3300 |
| LAG | 801 |
| CONSUMER-ID | `-` (« Consumer group 'explo-earliest' has no active members ») |

Relevé quelques minutes après l'arrêt ; le lag augmente car le producteur continue d'écrire alors qu'aucun membre ne lit pour ce groupe. Le groupe existe toujours sans membre : ce n'est qu'un état côté broker.

**Q2.1** — Au premier arrêt, le groupe explo-earliest a commité ses offsets dans le topic interne `__consumer_offsets` ; au redémarrage, le consommateur reprend à cet offset commité et ne relit donc pas ce qu'il avait déjà traité. `auto.offset.reset` ne s'applique que lorsqu'il n'existe aucun offset commité pour la partition, c'est-à-dire uniquement au tout premier lancement d'un groupe neuf.

**Q2.2** — Première façon : lancer la nouvelle version avec un nouveau `group.id` et `auto.offset.reset=earliest`, qui relit tout l'historique sans toucher aux offsets du groupe existant. Seconde façon : repositionner les offsets du groupe avec `kafka-consumer-groups --reset-offsets --to-earliest --execute`, tous ses membres arrêtés. En production je choisirais le nouveau `group.id`, car il est réversible, ne coupe rien et n'efface pas la position du groupe existant.

### Mesure 6 — acquittement automatique contre manuel, arrêt brutal (`kill -9`)

| Groupe | Mode | Lus avant le kill (dernier `lus=`) | CURRENT-OFFSET total après le kill | Relus au redémarrage |
|---|---|---|---|---|
| explo-auto | automatique | 13350 | 13308 | ≥ 42 |
| explo-manuel | manuel (tous les 100) | 25700 | 25700 | 0 à 49 |

- explo-auto : 13308 = 1286+118+3504+2100+3150+3150. Au moins 42 messages lus n'étaient pas commités (le dernier `lus=` affiché n'est qu'une borne basse, l'affichage se faisant tous les 50 messages). Reprise vérifiée exactement aux offsets commités (partition 0 repart à 3504). Juste après le kill, `--describe` affiche encore `lecteur-c1` comme membre : le broker attend l'expiration de `session.timeout.ms`.
- explo-manuel : 25700 = 2319+118+6603+4164+6249+6247. Le dernier commit a eu lieu à 25700 et le kill est survenu avant le `lus=25750` suivant. Reprise vérifiée juste après l'offset commité 6603 en partition 0.
- En manuel, l'écart lu/commité est borné à moins de 100 messages et connu précisément ; en automatique, il dépend du minutage de l'intervalle de 5 s.

**Q2.3** — Le risque de perte est du côté de l'acquittement automatique : le client commite périodiquement ce qui a été remis à l'application, traité ou non, donc si le processus meurt après un commit mais avant d'avoir fini de traiter ces messages, leurs offsets ont avancé alors que le traitement n'a jamais eu lieu. Le risque de double traitement est du côté de l'acquittement manuel placé après le traitement : si le processus meurt entre le traitement et le commit, l'offset n'a pas avancé et ces messages sont relus au redémarrage. Autrement dit, acquitter avant de traiter donne at-most-once (perte possible), acquitter après avoir traité donne at-least-once (doublon possible). C'est pour cela que la partie 4 commite manuellement après l'écriture en base, et neutralise les doublons par une clé primaire.

---

## Partie 3 — Consumer groups et rééquilibrage

### Mesure 7 — attribution des partitions (stratégie `range`, groupe vitals-explo-groupe)

| Situation | cA | cB | cC |
|---|---|---|---|
| cA seul | 0, 1, 2, 3, 4, 5 | — | — |
| cA + cB | 0, 1, 2 | 3, 4, 5 | — |
| cA + cB + cC | 0, 1 | 2, 3 | 4, 5 |

À chaque arrivée, les membres en place affichent un RETRAIT de toutes leurs partitions puis une ATTRIBUTION à la même seconde (14:06:49 à l'arrivée de cB, 14:08:16 à l'arrivée de cC) : rééquilibrage eager, tout est rendu puis redistribué.

### Mesure 8 — débit par instance à trois

Période à trois : 14:08:16 → 14:26:48 (~1110 s, laissé bien plus que 60 s).

| Instance | Messages lus à trois (différence des `lus=`) | Patients couverts |
|---|---|---|
| cA | 5908 − 1027 = 4881 | P005, P010, P012 (p0) + P007 (p1) = 4 |
| cB | 4423 − 760 = 3663 | p2 vide + P003, P008, P009 (p3) = 3 |
| cC | 6110 − 0 = 6110 | P002, P006, P011 (p4) + P001, P004 (p5) = 5 |

Ramené à 60 s : cA ≈ 263, cB ≈ 197, cC ≈ 329. Par patient : 1220 / 1221 / 1222 → débit strictement proportionnel au nombre de patients.

**Q3.1** — Non, les trois instances ne lisent pas le même nombre de messages : cC en lit 6110, cA 4881 et cB 3663, soit exactement le ratio 5/4/3 de leurs patients (≈ 1220 messages par patient chacune). L'écart vient de la mesure 3 : le hachage a réparti les 12 patients de façon inégale (3/1/0/3/3/2), et cB a hérité de la partition 2 qui ne reçoit plus aucun patient ; la charge d'un consommateur dépend des clés portées par ses partitions, pas de sa puissance.

### Mesure 9 — un consommateur de trop

| Grandeur | Valeur |
|---|---|
| Nombre de partitions du topic | 6 |
| Nombre d'instances lancées | 7 (c1 à c7, en arrière-plan depuis un seul terminal) |
| Instances affichant `partitions=[aucune]` | 1 (c7) |
| `lus=` de cette instance après ~40 s | 0 |

Remarque : c3 a reçu la partition 2, qui ne reçoit plus aucun patient ; il affiche `lus=0` lui aussi.

**Q3.2** — La borne supérieure du parallélisme de consommation est le nombre de partitions du topic, puisqu'une partition n'est lue que par un seul membre du groupe : au-delà, les membres en trop restent sans partition, en attente. Pour la dépasser, il faudrait augmenter le nombre de partitions du topic (ou créer un nouveau topic plus large et y basculer les producteurs). Comme en question 1.3, cette modification ne déplace pas les messages déjà écrits : une partie des clés change de partition pour les messages futurs, et l'ordre par clé est cassé jusqu'à ce que la rétention efface les anciens messages.

### Mesure 10 — perte d'un consommateur

| Scénario | Délai avant réattribution | `session.timeout.ms` par défaut |
|---|---|---|
| A — Ctrl-C (arrêt propre, `close()`) | ~1 s | 45 000 ms |
| B — Ctrl-Z puis `kill -9` (arrêt brutal) | ~35 s | 45 000 ms |

- Scénario A : c'est cA qui a été arrêté proprement (RETRAIT à 14:33:59) et cB qui a récupéré les 6 partitions à 14:34:00. Rôles inversés par rapport à l'énoncé, mécanisme identique.
- Scénario B : instance gelée puis tuée vers 14:36:08 ; l'autre instance récupère les 6 partitions à 14:36:43. Pendant ce délai, les partitions 3, 4, 5 n'ont été lues par aucun membre.

**Q3.3** — Avec Ctrl-C, `close()` envoie une demande de départ explicite (LeaveGroup) au coordinateur, qui déclenche immédiatement le rééquilibrage : ~1 s. Avec Ctrl-Z puis `kill -9`, le processus n'envoie ni départ ni heartbeat ; le coordinateur ne le déclare mort qu'après l'expiration de `session.timeout.ms` (45 s par défaut, ~35 s mesurés ici), et les partitions orphelines ne sont lues par personne pendant ce temps. On ne peut pas réduire ce délai à 1 s, car un membre vivant mais momentanément ralenti (pause du ramasse-miettes, poste chargé, micro-coupure réseau) serait exclu à tort, et chaque exclusion provoque un rééquilibrage qui interrompt tout le groupe ; le broker impose d'ailleurs un plancher de 6 s (`group.min.session.timeout.ms`).

### Mesure 11 — stratégie d'attribution, départ de cB

| Stratégie | Lignes RETRAIT chez cA au départ de cB | Partitions listées dans ce RETRAIT |
|---|---|---|
| range | 1 | toutes celles de l'instance restante : [3, 4, 5] (puis ATTRIBUTION [0..5]) |
| cooperative-sticky (groupe neuf vitals-explo-coop) | 0 | aucune : cA reçoit seulement [2, 1, 0] en plus à 14:40:52 et garde [3, 4, 5] sans interruption |

À l'arrivée de cB en coopératif, cA ne rend que [0, 1, 2] et continue de lire la partition 5 juste après ; en range, il rendait ses 6 partitions.

**Q3.4** — Avec la stratégie coopérative, un consommateur ne rend que les partitions qui changent réellement de propriétaire : il continue d'écrire en base pour les autres sans interruption, au lieu de tout suspendre pendant le rééquilibrage. Il n'a donc à vider son lot et commiter dans `on_revoke` que pour les partitions retirées, ce qui réduit la pause, les relectures et les doublons à absorber.

**Stratégie retenue dans le code de rendu** : `lecteur.py` garde `range` par défaut (paramétrable par `ASSIGNMENT_STRATEGY`) ; `consumer.py` utilise `cooperative-sticky`, comme l'impose l'énoncé.

---

## Partie 4 — Persistance PostgreSQL

### Décisions

| Décision | Valeur |
|---|---|
| Clé d'idempotence | `pk_vitals_raw PRIMARY KEY (kafka_partition, kafka_offset)` |
| Clause de conflit | `ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING` |
| Mode d'acquittement | `enable.auto.commit = False`, commit après `inserer_lot()` |
| Stratégie d'attribution | `cooperative-sticky` |
| Taille de lot / délai | `BATCH_SIZE=200`, `BATCH_TIMEOUT_S=5.0` |
| Point de départ d'un groupe neuf | `earliest` |

### Mesure 12 — service PostgreSQL

Image `cgr.dev/chainguard/postgres` (PostgreSQL 18.6, figée par digest), port publié 55432, schéma monté en lecture seule dans `/var/lib/postgres/initdb/01-init.sql`.

| Grandeur | Valeur |
|---|---|
| Délai entre `up -d postgres` et healthy | ~5 s (première sonde `pg_isready` réussie ; psql répondait dès les premières secondes) |
| Nombre de tables listées par `\dt` | 1 (`vitals_raw`) |
| Contrainte de clé primaire affichée par `\d vitals_raw` | `pk_vitals_raw` PRIMARY KEY (kafka_partition, kafka_offset) |

Également présents : index `idx_vitals_patient_time (patient_id, event_time DESC)` et contrainte `ck_vital_type`.

**Q4.1** — La clé primaire porte sur `(kafka_partition, kafka_offset)` parce que ce couple est attribué une seule fois à chaque message par Kafka et reste identique quand on le relit : il identifie le message, pas son contenu. Une clé `(patient_id, event_time, vital_type)` identifierait un contenu métier, et fusionnerait silencieusement deux messages légitimes distincts. Cas concret : un moniteur qui émet deux relevés HR pour P007 horodatés à la même milliseconde (deux capteurs, ou une correction de mesure) ; le second serait rejeté par `ON CONFLICT DO NOTHING` et perdu sans aucune erreur.

### Mesure 13 — consommateur lancé depuis l'hôte (~2 min 30)

| Grandeur | Valeur |
|---|---|
| `lus=` à l'arrêt | 71 233 |
| `inseres=` à l'arrêt | 71 233 (ignores_doublons=0, rejets=0, débit moyen 485,8 msg/s) |
| `lignes_totales` | 71 233 |
| `messages_distincts` | 71 233 |
| `patients_distincts` | 12 |

Par partition (count / min / max) : p0 17985/0/17984, p1 6113/0/6112, p2 118/0/117, p3 17632/0/17631, p4 17631/0/17630, p5 11754/0/11753 → count = max − min + 1 partout, aucun trou.
Comportement observé : ~69 400 messages d'historique rattrapés en ~7 s par lots pleins de 200, puis un lot toutes les 5 s de 49 à 96 messages (déclenchement par le délai).

**Q4.2** — Oui, `lignes_totales` et `messages_distincts` sont égaux (71 233) : chaque couple (kafka_partition, kafka_offset) n'apparaît qu'une fois, aucun message n'a été écrit deux fois. S'ils ne l'étaient pas, cela voudrait dire que la clé primaire ne protège pas l'identité du message (clé absente ou mal posée), et que des relectures ont créé des doublons en base.

### Mesure 14 — chaîne complète en conteneurs

| Grandeur | Valeur |
|---|---|
| Services affichés par `docker compose ps` | 4 (kafka, postgres, producer, consumer) |
| Services en état healthy | 3 (kafka, postgres, producer ; consumer n'a pas de healthcheck déclaré) |
| Taille moyenne des lots (`LOT taille=`) en régime normal | ~70 (de 60 à 95, un lot toutes les 5 s) |
| Croissance de `lignes_totales` sur 60 s | 780 lignes (72 433 → 73 213), soit 13 msg/s |

Au démarrage, le consumer conteneurisé s'est connecté à PostgreSQL au 1er essai et a repris le groupe `vitals-persister` là où le test hôte s'était arrêté (~1200 messages de retard rattrapés en lots de 200).

**Q4.3** — Le producteur émet ~13 msg/s (mesuré en S1 et confirmé ici : 780 lignes/min). Remplir un lot de `BATCH_SIZE=200` prendrait 200 / 13 ≈ 15 s, bien plus que `BATCH_TIMEOUT_S = 5 s`. C'est donc le délai qui déclenche en pratique, avec des lots d'environ 13 × 5 ≈ 65 messages (60 à 95 observés) ; la taille de 200 n'est atteinte que lors des rattrapages.

---

## Partie 5 — Résilience et mesure

### Mesure 15 — lag en régime nominal

| Instant | Somme des LAG |
|---|---|
| t0 (14:54:31) | 95 |
| t0 + 30 s | 64 |
| t0 + 60 s | 48 |

**Q5.1** — Le lag est stable : il oscille en dents de scie entre quelques dizaines et une centaine de messages sans tendance à la hausse, donc le consommateur tient le débit. À l'équilibre, comme on acquitte un lot toutes les `BATCH_TIMEOUT_S = 5 s` et que le producteur émet ~13 msg/s, le lag remonte jusqu'à ~13 × 5 ≈ 65 messages (jusqu'à ~95 pour les plus gros lots observés) juste avant un acquittement et retombe près de 0 juste après ; un relevé ponctuel tombe donc entre 0 et ~100, soit ~30 à 50 en moyenne.

### Mesure 16 — lag en surcharge

`INTERVAL_S=0.005` (une salve de ~13 relevés toutes les 5 ms, soit ~2 600 msg/s visés), `BATCH_SIZE=1`, consumer bridé à 5 % d'un cœur. Surcharge vérifiée dans le conteneur : `INTERVAL_S=0.005`.

| Instant | Somme des LAG | Écart avec le relevé précédent |
|---|---|---|
| t0 (14:56:37) | 16 989 | — |
| t0 + 30 s | 82 822 | +65 833 |
| t0 + 60 s | 152 327 | +69 505 |
| t0 + 120 s | 276 110 | +123 783 |

Croissance linéaire : pente ≈ 2 200 msg/s = débit d'entrée − débit de sortie.

### Mesure 17 — passage à deux consommateurs

| Grandeur | Valeur |
|---|---|
| Somme des LAG juste avant le `--scale` (14:59:32) | 388 973 |
| Somme des LAG ~60 s après (15:00:56) | 562 922 |
| Membres du groupe affichés par `--describe` | 2 (consumer-2 : partitions 0, 1, 2 ; consumer-1 : partitions 3, 4, 5) |
| Évolution du lag | continue de croître : pente ≈ 2 070 msg/s contre ≈ 2 200 à un consommateur, gain marginal |

**Q5.2** — On examine d'abord le consommateur lui-même : ici il traite un message par INSERT et par commit (`BATCH_SIZE=1`) sur 5 % d'un cœur, son coût unitaire est énorme, et le doubler ne fait que doubler un débit dérisoire face à ~2 600 msg/s en entrée. On vérifie ensuite si le nombre de partitions borne le parallélisme : avec 6 partitions on peut monter jusqu'à 6 consommateurs, pas au-delà. Enfin on regarde si la ressource partagée en aval, PostgreSQL, est saturée : c'est la cause qu'on ne peut pas corriger en ajoutant des consommateurs, puisque chaque consommateur supplémentaire ajoute de la contention sur la même base au lieu d'en retirer.

Retour à l'état nominal : `unset INTERVAL_S BATCH_SIZE`, `--scale consumer=1` ; vérifié `INTERVAL_S=1.0` et `NanoCpus=0` (plus de bridage). Rattrapage observé : lag 1 001 199 → 35 entre 15:06 et 15:09:56 (~5 500 msg/s de vidage), sans intervention.

### Mesure 18 — stockage indisponible

| Observation | Valeur |
|---|---|
| Le conteneur consommateur s'arrête-t-il ou redémarre-t-il ? | Il retente 30 fois (~60 s) puis sort en erreur (`RuntimeError: base injoignable`) ; `restart: unless-stopped` le relance automatiquement (« Up 22 seconds » au relevé) et la nouvelle instance reprend ses tentatives |
| Première ligne d'erreur | `ERREUR base: server closed the connection unexpectedly -> lot conserve, offsets non acquittes`, puis `PostgreSQL indisponible (essai n/30): could not translate host name "postgres"` |
| Somme des LAG après 90 s de coupure | 1 201 |
| `lignes_totales` avant la coupure | 1 264 093 |
| `lignes_totales` 60 s après le redémarrage de PostgreSQL | 1 266 241 (+2 148) |
| Somme des LAG 60 s après le redémarrage | 71 |
| Rattrapage sans intervention ? | Oui |

**Q5.3** — Non, les offsets du groupe `vitals-persister` n'ont pas avancé pendant la coupure : le lag est monté à 1 201, preuve visible que rien n'a été acquitté. C'est l'effet de l'ordre `inserer_lot()` puis `commit()` : l'insertion échoue, l'exception saute le commit, le lot reste en mémoire (puis est relu depuis Kafka après le redémarrage du conteneur), donc tout est réécrit au retour de la base. Dans l'ordre inverse, les offsets auraient avancé avant l'échec de l'insertion : les messages de la coupure n'auraient jamais été relus, soit une perte définitive et silencieuse.

### Mesure 19 — reprise après SIGKILL, preuve chiffrée

| Grandeur | Valeur |
|---|---|
| `lignes_totales` avant le SIGKILL | 1 266 937 |
| `inseres=` de l'instance relancée (ligne `Arret.`, après ~90 s) | 1 272 |
| `ignores_doublons=` de l'instance relancée | 0 |
| Lignes renvoyées par la requête de doublons | 0 |
| Partitions où lignes ≠ étendue | aucune |

Requête de trous : p0 317 247 / 317 247, p1 105 868 / 105 868, p2 118 / 118, p3 316 893 / 316 893, p4 316 893 / 316 893, p5 211 262 / 211 262 (lignes / étendue). Juste après le kill, `--describe` affichait encore l'ancien membre : le broker attend `session.timeout.ms`.

**Q5.4** — `ignores_doublons` vaut 0 ici : le SIGKILL est survenu alors que le lot en cours était encore en mémoire, ni écrit en base ni acquitté, donc les messages relus au redémarrage n'existaient pas encore dans la table et ont été insérés normalement. Des doublons interceptés n'apparaissent que si le crash tombe entre `inserer_lot()` et `commit()` (fenêtre de quelques millisecondes), ou lors d'un repositionnement volontaire en arrière (`--reset-offsets --shift-by`) : le lot relu est alors déjà en base et la clé primaire le refuse via `ON CONFLICT DO NOTHING`. Une valeur non nulle ne serait donc pas un défaut mais la preuve que des messages ont été relus et absorbés sans créer de ligne ; la preuve d'absence de perte et de doublon est donnée par les deux requêtes (0 doublon, lignes = étendue sur chaque partition).

**Q5.5** — Sur le flux, la chaîne est at-least-once : on acquitte après avoir écrit, donc un message n'est jamais perdu mais peut être relu après une panne (SIGKILL, coupure de base). Dans la table, l'état final est celui d'un traitement unique, car la clé primaire `(kafka_partition, kafka_offset)` avec `ON CONFLICT DO NOTHING` rend la réécriture idempotente : c'est de l'effectively-once. Ce n'est pas de l'exactly-once au sens Kafka, dont les transactions ne couvrent que Kafka vers Kafka et pas une base externe comme PostgreSQL.

---

## Nettoyage

- Schéma sauvegardé dans `sql/schema_dump.sql` (`pg_dump --schema-only`), non inclus dans le rendu.
- `docker compose down -v` : conteneurs, réseau et volumes `vitals-logs`, `vitals-kafka-data`, `pg-data` supprimés ; `docker volume ls` ne les liste plus.
- Kafka relancé seul sur un volume neuf : `kafka-consumer-groups --list` ne renvoie aucun groupe.

**Q6.1** — Après `docker compose down -v`, le volume Kafka est détruit avec le topic interne `__consumer_offsets` : les offsets commités du groupe `vitals-persister` ont disparu, tout comme le topic `mimic-vitals` lui-même. Le volume `pg-data` est détruit aussi : toutes les lignes de `vitals_raw` sont perdues, et au prochain démarrage le script d'initialisation recrée une table vide. Au prochain `up` (après recréation du topic, l'auto-création étant désactivée), le consommateur arrive dans un groupe sans offset commité : `AUTO_OFFSET_RESET=earliest` s'applique et il relit depuis le début tout ce que le topic contient, en repartant d'une table vide.
