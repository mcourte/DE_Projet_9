# ==============================================================================
# PROJET INDUTECH - DOCKERFILE PYSPARK CONSUMER
# Rôle : Exécuter pyspark_consumer.py (Structured Streaming) qui lit le topic
#        'client_tickets' depuis Redpanda et écrit les résultats en Parquet/JSON.
# ==============================================================================

FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

# Dépendances Python (pyspark est déjà présent dans l'image bitnami)
COPY requierements.txt ./requierements.txt
RUN pip install --no-cache-dir -r requierements.txt

# Code applicatif
COPY spark/pyspark_consumer.py ./pyspark_consumer.py

# Volumes pour la persistance des sorties et checkpoints
RUN mkdir -p /tmp/indutech/output /tmp/indutech/checkpoint

# Connecteur Kafka téléchargé automatiquement via --packages
ENV KAFKA_BROKER=redpanda-0:9092 \
    KAFKA_TOPIC=client_tickets \
    OUTPUT_PATH=/tmp/indutech/output \
    CHECKPOINT_PATH=/tmp/indutech/checkpoint \
    OUTPUT_FORMAT=parquet

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--master", "local[*]", \
     "/app/pyspark_consumer.py"]
