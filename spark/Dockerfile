# ==============================================================================
# PROJET INDUTECH - DOCKERFILE PYSPARK CONSUMER
# Rôle : Exécuter pyspark_consumer.py (Structured Streaming) qui lit le topic
#        'client_tickets' depuis Redpanda, enrichit les tickets et persiste
#        les résultats dans MySQL + Parquet.
# ==============================================================================

FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

# Dépendances Python
COPY requierements.txt ./requierements.txt
RUN pip install --no-cache-dir -r requierements.txt

# Code applicatif
COPY spark/pyspark_consumer.py ./pyspark_consumer.py

# Volumes pour la persistance des sorties et checkpoints
RUN mkdir -p /tmp/indutech/output /tmp/indutech/checkpoint

# Variables d'environnement
ENV KAFKA_BROKER=redpanda-0:9092 \
    KAFKA_TOPIC=client_tickets \
    OUTPUT_PATH=/tmp/indutech/output \
    CHECKPOINT_PATH=/tmp/indutech/checkpoint \
    MYSQL_HOST=mysql \
    MYSQL_PORT=3306 \
    MYSQL_DB=indutech \
    MYSQL_USER=indutech \
    MYSQL_PASSWORD=indutech

# Les JARs (Kafka connector + MySQL JDBC) sont téléchargés via --packages
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.4.0", \
     "--master", "local[*]", \
     "/app/pyspark_consumer.py"]
