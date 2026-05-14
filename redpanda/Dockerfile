# ==============================================================================
# DOCKERFILE : REDPANDA CUSTOM (INDUTECH)
# ==============================================================================

# 1. Utilisation de l'image officielle Redpanda
FROM docker.redpanda.com/redpandadata/redpanda:latest

# 2. Exposition des ports essentiels
# 9092 : API Kafka (Interne/Docker)
# 19092 : API Kafka (Externe/Host)
# 8081 : Schema Registry
# 9644 : Admin API
EXPOSE 9092 19092 8081 9644

# 3. Script d'initialisation pour créer le topic automatiquement
# On crée un petit script shell qui attend que Redpanda soit prêt 
# pour lancer la commande 'rpk topic create'
RUN echo '#!/bin/bash \n\
redpanda start --mode dev-container --smp 1 --memory 1G --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 --advertise-kafka-addr internal://redpanda-0:9092,external://localhost:19092 & \n\
until rpk cluster info; do sleep 2; done \n\
rpk topic create client_tickets \n\
wait' > /usr/bin/entrypoint-custom.sh

RUN chmod +x /usr/bin/entrypoint-custom.sh

# 4. Lancement via le script personnalisé
ENTRYPOINT ["/usr/bin/entrypoint-custom.sh"]