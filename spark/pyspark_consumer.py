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
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# ------------------------------------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda-0:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "client_tickets")

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/indutech/output")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/tmp/indutech/checkpoint")

# MySQL - connexion JDBC
MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB = os.environ.get("MYSQL_DB", "indutech")
MYSQL_USER = os.environ.get("MYSQL_USER", "indutech")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "indutech")

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
MYSQL_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}


# ------------------------------------------------------------------------------
# 2. INITIALISATION (SparkSession)
# ------------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("InduTechTicketAnalysis")
    # Performance : partitions de shuffle et memory tuning
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.minBatchesToRetain", "20")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ------------------------------------------------------------------------------
# 3. SCHEMA & LECTURE DU FLUX
# ------------------------------------------------------------------------------
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
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)


# ------------------------------------------------------------------------------
# 4. TRANSFORMATIONS (Logique Métier)
# ------------------------------------------------------------------------------
tickets = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
    .select(from_json(col("json_value"), ticket_schema).alias("t"), col("kafka_ts"))
    .select("t.*", "kafka_ts")
    .withColumn("event_time", to_timestamp(col("created_at")))
)

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
# 5. SINK MYSQL (avec résilience)
# ------------------------------------------------------------------------------
def write_to_mysql_with_retry(
    df: DataFrame,
    table: str,
    max_retries: int = 5,
    backoff: float = 2.0,
) -> None:
    """Écrit le DataFrame dans MySQL avec retry exponentiel.

    En cas de déconnexion MySQL, on retente jusqu'à max_retries fois ;
    si tout échoue, on relance l'exception pour que Spark enregistre
    l'erreur — le checkpoint garantit que le micro-batch sera rejoué.
    """
    attempt = 0
    while True:
        try:
            (
                df.write
                .mode("append")
                .jdbc(url=MYSQL_URL, table=table, properties=MYSQL_PROPS)
            )
            return
        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                print(f"[ERREUR] MySQL table={table} echec definitif : {exc}")
                raise
            wait = backoff ** attempt
            print(
                f"[WARN] MySQL table={table} tentative {attempt}/{max_retries} "
                f"echec ({exc}), retry dans {wait:.1f}s"
            )
            time.sleep(wait)


def foreach_batch_tickets(batch_df: DataFrame, batch_id: int) -> None:
    """Écrit les tickets enrichis dans MySQL + Parquet (backup)."""
    if batch_df.rdd.isEmpty():
        return
    batch_df = batch_df.drop("kafka_ts")
    batch_df.persist()
    try:
        write_to_mysql_with_retry(batch_df, table="tickets")
        (
            batch_df.write
            .mode("append")
            .parquet(f"{OUTPUT_PATH}/tickets")
        )
        print(f"[OK] batch={batch_id} tickets ecrits (count={batch_df.count()})")
    finally:
        batch_df.unpersist()


def foreach_batch_agg(batch_df: DataFrame, batch_id: int) -> None:
    """Écrit les agrégations par type dans MySQL + Parquet."""
    if batch_df.rdd.isEmpty():
        return
    batch_df.persist()
    try:
        write_to_mysql_with_retry(batch_df, table="tickets_agg_par_type")
        (
            batch_df.write
            .mode("append")
            .parquet(f"{OUTPUT_PATH}/agg_par_type")
        )
        print(f"[OK] batch={batch_id} agregations ecrites (count={batch_df.count()})")
    finally:
        batch_df.unpersist()


# ------------------------------------------------------------------------------
# 6. STREAMING QUERIES
# ------------------------------------------------------------------------------
# Query 1 : tickets enrichis ligne à ligne -> MySQL.tickets + Parquet
query_tickets = (
    tickets_enrichis
    .writeStream
    .foreachBatch(foreach_batch_tickets)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/tickets")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

# Query 2 : agrégation par type sur fenêtre de 1 min -> MySQL.tickets_agg_par_type + Parquet
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
    .foreachBatch(foreach_batch_agg)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/agg_par_type")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

# Query 3 : monitoring console (aperçu temps réel)
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
