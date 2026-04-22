"""
================================================================================
PROJET INDUTECH - POC GESTION DE TICKETS (PRODUCER)
================================================================================

RÔLE : Simuler l'arrivée de tickets clients en temps réel et les envoyer
       vers le broker Redpanda.

ARCHITECTURE :
[ Source : Script Python ] ---> [ Topic : client_tickets ] ---> [ Broker : Redpanda ]
================================================================================
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ------------------------------------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------------------------------------
# Par défaut on pointe vers le port externe (exécution hors Docker).
# À l'intérieur du réseau Docker, surcharger via la variable d'environnement
# KAFKA_BROKER=redpanda-0:9092
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "client_tickets")

TYPES = ["technique", "facturation", "livraison", "autre"]
PRIORITES = ["basse", "moyenne", "haute"]

fake = Faker("fr_FR")


# ------------------------------------------------------------------------------
# 2. GÉNÉRATION DE DONNÉES (SCHEMA INDUTECH)
# ------------------------------------------------------------------------------
def generer_ticket() -> dict:
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "demande": fake.sentence(nb_words=12),
        "type": random.choice(TYPES),
        "priorite": random.choice(PRIORITES),
    }


# ------------------------------------------------------------------------------
# 3. LOGIQUE DU PRODUCTEUR
# ------------------------------------------------------------------------------
def creer_producer() -> KafkaProducer:
    """Crée un KafkaProducer avec sérialisation JSON (utf-8) et retry."""
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"[WARN] Broker {BROKER} indisponible, nouvelle tentative dans 5s...")
            time.sleep(5)


def on_success(record_metadata):
    print(
        f"[OK] topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_error(excp):
    print(f"[ERREUR] Echec de l'envoi : {excp}")


def main() -> None:
    print(f"[INIT] Connexion au broker {BROKER} sur le topic '{TOPIC}'")
    producer = creer_producer()
    print("[INIT] Producer prêt, démarrage de la boucle d'envoi.")

    try:
        while True:
            ticket = generer_ticket()
            try:
                future = producer.send(
                    TOPIC,
                    key=str(ticket["client_id"]),
                    value=ticket,
                )
                future.add_callback(on_success).add_errback(on_error)
                print(
                    f"[SEND] ticket_id={ticket['ticket_id'][:8]}... "
                    f"client={ticket['client_id']} "
                    f"type={ticket['type']} "
                    f"priorite={ticket['priorite']}"
                )
            except KafkaError as e:
                print(f"[ERREUR] KafkaError : {e}, tentative de reconnexion...")
                producer.close()
                producer = creer_producer()

            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print("\n[STOP] Interruption utilisateur, fermeture du producer...")
    finally:
        # 4. ROBUSTESSE : flush avant fermeture pour garantir l'envoi
        try:
            producer.flush(timeout=10)
            producer.close(timeout=10)
            print("[STOP] Producer fermé proprement.")
        except Exception as e:
            print(f"[ERREUR] Fermeture producer : {e}")


if __name__ == "__main__":
    main()
