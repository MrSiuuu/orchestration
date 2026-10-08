import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1.0"))

PLAGES = {
    "HR": (55, 115, "bpm"),
    "SPO2": (90, 100, "%"),
    "ABP_SYS": (95, 145, "mmHg"),
}

arret = False
echecs = 0


def gerer_signal(signum, frame):
    global arret
    arret = True


def creer_producteur():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulator-s1",
        "acks": "all",
        "linger.ms": 50,
        "retries": 5,
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
    })


def accuse_reception(err, msg):
    global echecs
    if err is not None:
        echecs += 1
        print(f"Echec livraison: {err}", file=sys.stderr)


def generer_releve(patient_id):
    vital_type = random.choice(list(PLAGES))
    mini, maxi, unite = PLAGES[vital_type]
    return {
        "patient_id": patient_id,
        "event_time": int(time.time() * 1000),
        "vital_type": vital_type,
        "value": round(random.uniform(mini, maxi), 1),
        "unit": unite,
    }


def main():
    signal.signal(signal.SIGINT, gerer_signal)
    signal.signal(signal.SIGTERM, gerer_signal)

    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]
    envoyes = 0
    debut = time.time()

    while not arret:
        for patient_id in patients:
            releve = generer_releve(patient_id)
            producteur.produce(
                topic=VITALS_TOPIC,
                key=patient_id.encode("utf-8"),
                value=json.dumps(releve).encode("utf-8"),
                on_delivery=accuse_reception,
            )
            envoyes += 1
        producteur.poll(0)
        time.sleep(INTERVAL_S)

    restants = producteur.flush(10)
    duree = time.time() - debut
    print(f"Bilan: envoyes={envoyes} non_livres={echecs + restants} "
          f"duree={duree:.1f}s debit={envoyes / duree:.1f} msg/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
