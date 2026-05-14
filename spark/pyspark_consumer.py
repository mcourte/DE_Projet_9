"""
================================================================================
PROJET INDUTECH - ANALYSE DE FLUX TEMPS RÉEL (PYSPARK CONSUMER)
================================================================================

RÔLE : Lire le flux de tickets depuis Redpanda, enrichir les données,
       calculer des statistiques en continu et persister les résultats
       dans MySQL (+ Parquet de secours).

ARCHITECTURE :
[ Redpanda ] ---> [ PySpark Streaming ] ---> [ MySQL + Parquet ]
================================================================================
"""

import os
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# --- 1. CONFIGURATION ---
# On récupère les adresses des serveurs (Kafka, MySQL) et les dossiers de stockage
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda-0:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "client_tickets")

# Dossiers pour sauvegarder les fichiers et l'état de Spark (Checkpoints)
OUTPUT_PATH = "/tmp/indutech/output"
CHECKPOINT_PATH = "/tmp/indutech/checkpoint"

# Paramètres pour se connecter à la base de données MySQL
MYSQL_URL = "jdbc:mysql://mysql:3306/indutech?useSSL=false"
MYSQL_PROPS = {"user": "indutech", "password": "indutech", "driver": "com.mysql.cj.jdbc.Driver"}

# --- 2. DÉMARRAGE DE SPARK ---
spark = (SparkSession.builder 
    .appName("InduTechAnalysis")
    .config("spark.sql.shuffle.partitions", "4") # On limite le nombre de tâches pour aller plus vite
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN") # On n'affiche que les messages importants

# --- 3. DÉFINITION DU MODÈLE (SCHEMA) ---
# On explique à Spark à quoi ressemble un ticket (ID, Client, Type, etc.)
ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type", StringType(), True),
    StructField("priorite", StringType(), True),
])

# --- 4. LECTURE DU FLUX ---
# Spark se connecte à Redpanda et "écoute" les nouveaux messages
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .load())

# --- 5. TRANSFORMATION DES DONNÉES ---
# On convertit le JSON brut en colonnes lisibles et on ajoute des infos
tickets = (raw_stream
    .select(from_json(col("value").cast("string"), ticket_schema).alias("t"))
    .select("t.*")
    .withColumn("event_time", to_timestamp(col("created_at"))))

# Enrichissement : on attribue une équipe à chaque type de ticket
tickets_enrichis = tickets.withColumn("equipe_support", 
    when(col("type") == "technique", "Equipe IT")
    .when(col("type") == "facturation", "Equipe Finance")
    .otherwise("Equipe Support Client")
)

# --- 6. RANGEMENT DES DONNÉES (SINK) ---

def enregistrer_donnees(batch_df, batch_id):
    """Fonction qui range chaque 'lot' de tickets dans MySQL et en Parquet."""
    if not batch_df.isEmpty():
        # A. On enregistre dans la base de données MySQL
        batch_df.write.mode("append").jdbc(url=MYSQL_URL, table="tickets", properties=MYSQL_PROPS)
        
        # B. On garde une copie de secours en format Parquet (très rapide à lire)
        batch_df.write.mode("append").parquet(f"{OUTPUT_PATH}/tickets")
        
        print(f"[OK] Batch {batch_id} : {batch_df.count()} tickets traités et enregistrés.")

# --- 7. LANCEMENT DE L'ANALYSE ---
# On dit à Spark de démarrer le travail toutes les 30 secondes
query = (tickets_enrichis.writeStream
    .foreachBatch(enregistrer_donnees) # Appelle la fonction de rangement
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/tickets") # Mémoire pour ne pas perdre le fil
    .trigger(processingTime="30 seconds")
    .start())

# On affiche aussi un aperçu dans le terminal pour nous rassurer
query_console = (tickets_enrichis.writeStream
    .format("console")
    .trigger(processingTime="30 seconds")
    .start())

spark.streams.awaitAnyTermination()