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

# --- CONFIGURATION ---
# On définit l'adresse du serveur (Broker) et le nom de la boîte aux lettres (Topic)
BROKER = os.environ.get("KAFKA_BROKER", "localhost:19092")
TOPIC = os.environ.get("KAFKA_TOPIC", "client_tickets")

TYPES = ["technique", "facturation", "livraison", "autre"]
PRIORITES = ["basse", "moyenne", "haute"]

fake = Faker("fr_FR") # Pour générer du texte en français

# --- GÉNÉRATION DE DONNÉES ---
def generer_ticket() -> dict:
    """Crée un dictionnaire représentant un ticket client."""
    return {
        "ticket_id": str(uuid.uuid4()), # Identifiant unique
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "demande": fake.sentence(nb_words=12), # Texte aléatoire
        "type": random.choice(TYPES),
        "priorite": random.choice(PRIORITES),
    }

# --- LOGIQUE D'ENVOI (KAFKA) ---
def creer_producer() -> KafkaProducer:
    """Initialise la connexion avec Redpanda/Kafka."""
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[BROKER],
                # Transforme automatiquement les dictionnaires Python en JSON pour l'envoi
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all", # Attend confirmation que le message est bien arrivé
                retries=5,  # Réessaie 5 fois en cas de petit bug réseau
            )
        except NoBrokersAvailable:
            print(f"[ATTENTE] Serveur indisponible, on réessaie dans 5s...")
            time.sleep(5)

def on_success(record_metadata):
    """S'exécute quand un message est bien reçu par Redpanda."""
    print(f"[OK] Envoyé : Topic={record_metadata.topic}, Offset={record_metadata.offset}")

def on_error(excp):
    """S'exécute si l'envoi échoue."""
    print(f"[ERREUR] Message non envoyé : {excp}")

# --- BOUCLE PRINCIPALE ---
def main() -> None:
    print(f"[START] Connexion à {BROKER}...")
    producer = creer_producer()

    try:
        while True: # Boucle infinie
            ticket = generer_ticket()
            
            # Envoi du ticket
            future = producer.send(
                TOPIC,
                key=str(ticket["client_id"]),
                value=ticket,
            )
            
            # Vérification du résultat (Succès ou Échec)
            future.add_callback(on_success).add_errback(on_error)
            
            print(f"[INFO] Nouveau ticket {ticket['ticket_id'][:8]} envoyé.")

            # Attend entre 1 et 5 secondes avant le prochain ticket
            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print("\n[STOP] Arrêt demandé par l'utilisateur.")
    finally:
        # On s'assure que tous les messages restants sont envoyés avant de quitter
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()