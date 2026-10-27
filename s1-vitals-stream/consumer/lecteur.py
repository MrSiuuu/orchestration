"""Lecteur exploratoire : lit mimic-vitals, compte et affiche. N'ecrit nulle part."""
import json
import os
import signal
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, KafkaException

# --- 1. Configuration par l'environnement ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() in ("1", "true", "yes")
INSTANCE = os.getenv("INSTANCE", "c1")
STRATEGIE = os.getenv("ASSIGNMENT_STRATEGY", "range")  # range ou cooperative-sticky (partie 3.4)

# --- 2. Etat partage ---
arret = False              # passe a True sur Ctrl-C ou docker stop
lus = 0                    # total de messages lus
par_partition = Counter()  # partition -> nombre de messages lus


# --- 3. Arret propre : on sort de la boucle pour passer par close() ---
def stop(signum, frame):
    global arret
    arret = True


signal.signal(signal.SIGINT, stop)   # Ctrl-C
signal.signal(signal.SIGTERM, stop)  # docker stop


# --- 4. Construction du consommateur ---
def creer_consommateur():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"lecteur-{INSTANCE}",
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": AUTO_COMMIT,
        "partition.assignment.strategy": STRATEGIE,
    })


# --- 5. Rappels de reequilibrage ---
def sur_attribution(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] ATTRIBUTION t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{liste}]", flush=True)


def sur_retrait(consommateur, partitions):
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] RETRAIT t={time.strftime('%H:%M:%S')} lus={lus} "
          f"partitions=[{liste}]", flush=True)


# --- 6. Boucle principale ---
def main():
    global lus
    c = creer_consommateur()
    # subscribe() n'attribue rien : l'attribution arrive au premier poll().
    c.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    print(f"[{INSTANCE}] demarrage groupe={GROUP_ID} reset={OFFSET_RESET} "
          f"auto_commit={AUTO_COMMIT} strategie={STRATEGIE}", flush=True)
    try:
        while not arret:
            msg = c.poll(1.0)  # timeout en SECONDES
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # fin de partition, pas une erreur
                print(f"[{INSTANCE}] erreur: {msg.error()}", flush=True)
                continue

            try:
                vital = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                vital = {}

            lus += 1
            par_partition[msg.partition()] += 1

            if lus % 50 == 0:
                print(f"[{INSTANCE}] lus={lus} p={msg.partition()} o={msg.offset()} "
                      f"patient={vital.get('patient_id')}", flush=True)

            if not AUTO_COMMIT and lus % 100 == 0:
                try:
                    c.commit(asynchronous=False)  # bloque jusqu'a confirmation
                except KafkaException as err:
                    print(f"[{INSTANCE}] commit impossible: {err}", flush=True)
    finally:
        print(f"[{INSTANCE}] Arret. lus={lus} "
              f"repartition={dict(sorted(par_partition.items()))}", flush=True)
        c.close()  # quitte le groupe proprement


if __name__ == "__main__":
    main()
