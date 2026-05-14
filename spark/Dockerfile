# ==============================================================================
# PROJET INDUTECH - DOCKERFILE PYSPARK (VERSION APACHE OFFICIELLE)
# ==============================================================================
FROM apache/spark:3.5.0

USER root

# Installation des librairies Python directement
RUN pip install --no-cache-dir kafka-python-ng pyspark

WORKDIR /app

# On copie le script (Assure-toi que le chemin est bon par rapport à ta racine)
# Si tu es dans DE_Projet_9, le fichier est dans spark/pyspark_consumer.py
COPY spark/pyspark_consumer.py ./pyspark_consumer.py

# Configuration des dossiers pour les checkpoints
RUN mkdir -p /tmp/indutech/output /tmp/indutech/checkpoint && \
    chmod -R 777 /tmp/indutech

# Variables d'environnement
ENV KAFKA_BROKER=redpanda-0:9092 \
    KAFKA_TOPIC=client_tickets \
    MYSQL_HOST=mysql \
    MYSQL_DB=indutech \
    MYSQL_USER=indutech \
    MYSQL_PASSWORD=indutech

# Commande de lancement (Chemin vers spark-submit propre à l'image Apache)
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.mysql:mysql-connector-j:8.4.0", \
     "--master", "local[*]", \
     "/app/pyspark_consumer.py"]