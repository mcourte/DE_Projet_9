"""
================================================================================
PROJET INDUTECH - ANALYSE DE FLUX TEMPS RÉEL (PYSPARK CONSUMER)
================================================================================

RÔLE : Lire le flux de tickets depuis Redpanda, enrichir les données
       et calculer des statistiques en continu (Structured Streaming).

ARCHITECTURE :
[ Redpanda ] ---> [ PySpark Streaming ] ---> [ Parquet / JSON Output ]
================================================================================
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import StringType, StructField, StructType, IntegerType


# ------------------------------------------------------------------------------
# 1. INITIALISATION (SparkSession)
# ------------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda-0:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "client_tickets")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/indutech/output")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/tmp/indutech/checkpoint")
OUTPUT_FORMAT = os.environ.get("OUTPUT_FORMAT", "parquet")  # parquet ou json

spark = (
    SparkSession.builder
    .appName("InduTechTicketAnalysis")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------------------------
# 2. LECTURE DU FLUX (Source)
# ------------------------------------------------------------------------------
# Schéma JSON correspondant au Producer (producer.py)
ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type", StringType(), True),
    StructField("priorite", StringType(), True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# ------------------------------------------------------------------------------
# 3. TRANSFORMATIONS (Logique Métier)
# ------------------------------------------------------------------------------
# Désérialisation : value (bytes) -> JSON -> colonnes typées
tickets = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
    .select(from_json(col("json_value"), ticket_schema).alias("t"), col("kafka_ts"))
    .select("t.*", "kafka_ts")
    .withColumn("event_time", to_timestamp(col("created_at")))
)

# Mapping équipe support + flag urgence
tickets_enrichis = (
    tickets
    .withColumn(
        "equipe_support",
        when(col("type") == "technique", "Tech")
        .when(col("type") == "facturation", "Finance")
        .when(col("type") == "livraison", "Logistique")
        .otherwise("Général"),
    )
    .withColumn("urgent", col("priorite") == "haute")
)


# ------------------------------------------------------------------------------
# 4. EXPORT DES RÉSULTATS (Sinks)
# ------------------------------------------------------------------------------
# Sink 1 : tickets enrichis bruts (ligne à ligne) en Parquet/JSON
query_raw = (
    tickets_enrichis
    .writeStream
    .format(OUTPUT_FORMAT)
    .option("path", f"{OUTPUT_PATH}/tickets")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/tickets")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

# Sink 2 : agrégation - nombre de tickets par type sur une fenêtre de 1 min
agg_par_type = (
    tickets_enrichis
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("type"),
        col("equipe_support"),
    )
    .count()
    .selectExpr(
        "window.start AS window_start",
        "window.end   AS window_end",
        "type",
        "equipe_support",
        "count AS nb_tickets",
    )
)

query_agg = (
    agg_par_type
    .writeStream
    .format(OUTPUT_FORMAT)
    .option("path", f"{OUTPUT_PATH}/agg_par_type")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/agg_par_type")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

# Sink 3 : monitoring console (aperçu temps réel)
query_console = (
    tickets_enrichis
    .select("ticket_id", "client_id", "type", "equipe_support", "priorite", "urgent")
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)


spark.streams.awaitAnyTermination()
