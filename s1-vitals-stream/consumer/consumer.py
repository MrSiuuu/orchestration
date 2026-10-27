"""Consommateur de persistance : mimic-vitals -> PostgreSQL vitals_raw.
Lots, ecriture idempotente, base d'abord puis acquittement Kafka."""
import json
import os
import signal
import socket
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError, KafkaException

# --- Configuration par l'environnement ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "vitals-persister")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5.0"))
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "55432"))
PG_DB = os.getenv("PG_DB", "healthstream")
PG_USER = os.getenv("PG_USER", "hs_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "hs_password")
INSTANCE = socket.gethostname()  # unique par conteneur (le PID vaut souvent 1)

TYPES_VALIDES = {"HR", "SPO2", "ABP_SYS"}

# Une seule instruction pour tout le lot ; un message deja ecrit est ignore.
SQL_INSERT = (
    "INSERT INTO vitals_raw "
    "(kafka_partition, kafka_offset, patient_id, event_time, vital_type, value, unit) "
    "VALUES %s "
    "ON CONFLICT ON CONSTRAINT pk_vitals_raw DO NOTHING"
)

arret = False


def stop(signum, frame):
    global arret
    arret = True


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def log(texte):
    print(f"[{INSTANCE}] {time.strftime('%H:%M:%S')} {texte}", flush=True)


def creer_consommateur() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": f"persister-{INSTANCE}",
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": False,                          # acquittement manuel
        "partition.assignment.strategy": "cooperative-sticky",
        "max.poll.interval.ms": 300000,                       # > temps d'ecriture d'un lot
    })


def connecter_postgres(tentatives: int = 30, delai_s: float = 2.0):
    for essai in range(1, tentatives + 1):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASSWORD,
                                    connect_timeout=5)
            conn.autocommit = False  # transactions explicites
            log(f"PostgreSQL connecte (essai {essai})")
            return conn
        except psycopg2.OperationalError as err:
            log(f"PostgreSQL indisponible (essai {essai}/{tentatives}): "
                f"{str(err).strip()}")
            time.sleep(delai_s)
    raise RuntimeError("base injoignable")


def ligne_depuis_message(msg) -> tuple:
    v = json.loads(msg.value())
    if v["vital_type"] not in TYPES_VALIDES:
        raise ValueError(f"vital_type inconnu: {v['vital_type']}")
    return (
        msg.partition(),
        msg.offset(),
        str(v["patient_id"]),
        datetime.fromtimestamp(int(v["event_time"]) / 1000, tz=timezone.utc),
        str(v["vital_type"]),
        float(v["value"]),
        str(v["unit"]),
    )


def inserer_lot(conn, lignes: list) -> int:
    with conn.cursor() as cur:
        execute_values(cur, SQL_INSERT, lignes, page_size=len(lignes))
        inserees = cur.rowcount  # lignes reellement inserees (doublons exclus)
    conn.commit()
    return inserees


def main() -> int:
    consommateur = creer_consommateur()
    consommateur.subscribe([TOPIC])
    conn = connecter_postgres()

    lot = []            # lignes en attente d'ecriture
    en_attente = 0      # messages lus non acquittes (lot + rejets)
    lus = inseres = ignores = rejets = 0
    debut = dernier_envoi = time.time()
    log(f"demarrage groupe={GROUP_ID} batch={BATCH_SIZE} timeout={BATCH_TIMEOUT_S}s")

    try:
        while not arret:
            msg = consommateur.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log(f"erreur Kafka: {msg.error()}")
                else:
                    lus += 1
                    en_attente += 1
                    try:
                        lot.append(ligne_depuis_message(msg))
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as err:
                        rejets += 1  # malforme : compte, ignore, mais acquitte
                        log(f"REJET p={msg.partition()} o={msg.offset()}: {err}")

            age = time.time() - dernier_envoi
            lot_pret = len(lot) >= BATCH_SIZE or (en_attente > 0 and age >= BATCH_TIMEOUT_S)
            if not lot_pret:
                continue

            try:
                n = inserer_lot(conn, lot) if lot else 0      # 1. la base d'abord
                consommateur.commit(asynchronous=False)        # 2. l'acquittement ENSUITE
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as err:
                log(f"ERREUR base: {str(err).strip()} -> lot conserve, offsets non acquittes")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connecter_postgres()  # lot CONSERVE, on reessaie au tour suivant
                continue
            except KafkaException as err:
                # lot deja ecrit : une relecture sera absorbee par la cle primaire
                log(f"commit Kafka impossible: {err}")
                n = n if "n" in dir() else 0

            inseres += n
            ignores += len(lot) - n
            log(f"LOT taille={len(lot)} inseres={n} doublons={len(lot) - n} lus={lus}")
            lot, en_attente, dernier_envoi = [], 0, time.time()
    finally:
        # Arret propre : ecrire le residuel, acquitter, quitter le groupe.
        if lot or en_attente:
            try:
                n = inserer_lot(conn, lot) if lot else 0
                consommateur.commit(asynchronous=False)
                inseres += n
                ignores += len(lot) - n
            except Exception as err:
                log(f"lot residuel non ecrit ({err}) : il sera relu au redemarrage")
        duree = max(time.time() - debut, 1e-9)
        log(f"Arret. lus={lus} inseres={inseres} ignores_doublons={ignores} "
            f"rejets={rejets} debit={lus / duree:.1f} msg/s")
        consommateur.close()
        try:
            conn.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
