# ==============================================================================
# PROJET INDUTECH - DOCKERFILE PRODUCER
# Rôle : Exécuter en continu le script producer.py qui génère des tickets
#        clients et les envoie vers Redpanda.
# ==============================================================================

FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances Python
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Code applicatif
COPY producer/producer.py ./producer.py

# Broker interne par défaut (réseau docker-compose)
ENV KAFKA_BROKER=redpanda-0:9092 \
    KAFKA_TOPIC=client_tickets

CMD ["python", "producer.py"]
