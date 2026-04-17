"""
================================================================================
PROJET INDUTECH - POC GESTION DE TICKETS (PRODUCER)
================================================================================

RÔLE : Simuler l'arrivée de tickets clients en temps réel et les envoyer
       vers le broker Redpanda.

ARCHITECTURE :
[ Source : Script Python ] ---> [ Topic : client_tickets ] ---> [ Broker : Redpanda ]

--------------------------------------------------------------------------------
TODO LIST DU DÉVELOPPEUR :
--------------------------------------------------------------------------------
1. CONFIGURATION
   [ ] Broker : localhost:19092 (externe) ou redpanda-0:9092 (docker)
   [ ] Topic  : "client_tickets"

2. GÉNÉRATION DE DONNÉES (SCHEMA INDUTECH)
   [ ] ticket_id  : UUID (chaîne unique)
   [ ] client_id  : Int (ID client aléatoire)
   [ ] created_at : Timestamp ISO8601
   [ ] demande    : Texte (ex: via Faker ou liste de phrases)
   [ ] type       : ['technique', 'facturation', 'livraison', 'autre']
   [ ] priorite   : ['basse', 'moyenne', 'haute']

3. LOGIQUE DU PRODUCTEUR
   [ ] Initialiser KafkaProducer avec sérialiseur JSON (utf-8)
   [ ] Boucle infinie : while True
   [ ] Envoi asynchrone : producer.send()
   [ ] Latence simulée : time.sleep(1 a 5 sec)

4. ROBUSTESSE
   [ ] Try/Except pour gérer les déconnexions de Redpanda
   [ ] Flush() pour garantir l'envoi avant fermeture
================================================================================
"""