from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def run_job1() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    trips_topic = os.getenv("TRIPS_TOPIC", "trips.raw")
    delta_base = os.getenv("DELTA_BASE_PATH", "data/delta")
    checkpoint_base = os.getenv("CHECKPOINT_BASE_PATH", "data/checkpoints")

    spark = build_spark("job1-bronze-silver")

    schema = StructType(
        [
            StructField("ingestion_id", StringType(), False),
            StructField("row_number", IntegerType(), False),
            StructField("trip_id", StringType(), False),
            StructField("region", StringType(), False),
            StructField("origin_lon", DoubleType(), False),
            StructField("origin_lat", DoubleType(), False),
            StructField("destination_lon", DoubleType(), False),
            StructField("destination_lat", DoubleType(), False),
            StructField("datetime", StringType(), False),
            StructField("datasource", StringType(), False),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", trips_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw_stream.withColumn("payload", F.from_json(F.col("value").cast("string"), schema))

    bronze = parsed.select(
        F.col("payload.*"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    is_valid = (
        (F.col("origin_lon").between(-180.0, 180.0))
        & (F.col("origin_lat").between(-90.0, 90.0))
        & (F.col("destination_lon").between(-180.0, 180.0))
        & (F.col("destination_lat").between(-90.0, 90.0))
        & F.col("trip_id").isNotNull()
    )

    silver = (
        bronze.filter(is_valid)
        .withColumn("event_time", F.to_timestamp("datetime"))
        .withWatermark("event_time", "1 day")
        .dropDuplicates(["trip_id"])
        .drop("event_time")
    )

    rejected = bronze.filter(~is_valid)

    (
        bronze.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/bronze_trips")
        .outputMode("append")
        .start(f"{delta_base}/bronze_trips")
    )

    (
        silver.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/silver_trips")
        .outputMode("append")
        .start(f"{delta_base}/silver_trips")
    )

    (
        rejected.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/rejected_trips")
        .outputMode("append")
        .start(f"{delta_base}/rejected_trips")
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_job1()
