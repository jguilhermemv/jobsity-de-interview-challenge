from __future__ import annotations

import os
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from de_challenge.spark.status_publisher import publish_ingestion_status


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


# Flexible schema: trip rows have trip fields; marker rows have record_type, control_id, total_rows
_FLEXIBLE_SCHEMA = StructType(
    [
        StructField("record_type", StringType(), True),
        StructField("ingestion_id", StringType(), True),
        StructField("control_id", StringType(), True),
        StructField("total_rows", IntegerType(), True),
        StructField("row_number", IntegerType(), True),
        StructField("trip_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("origin_lon", DoubleType(), True),
        StructField("origin_lat", DoubleType(), True),
        StructField("destination_lon", DoubleType(), True),
        StructField("destination_lat", DoubleType(), True),
        StructField("datetime", StringType(), True),
        StructField("datasource", StringType(), True),
    ]
)

def _is_marker() -> Any:
    return F.col("record_type") == "ingestion_end"


def _is_trip() -> Any:
    return F.col("record_type").isNull() | (F.col("record_type") == "trip")


def _is_valid_trip() -> Any:
    return (
        (F.col("origin_lon").between(-180.0, 180.0))
        & (F.col("origin_lat").between(-90.0, 90.0))
        & (F.col("destination_lon").between(-180.0, 180.0))
        & (F.col("destination_lat").between(-90.0, 90.0))
        & F.col("trip_id").isNotNull()
    )


def _build_silver_trips_df(valid_trips: Any) -> Any:
    return (
        valid_trips.withColumn("raw_payload", F.col("value").cast("string"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        .select(
            F.col("ingestion_id"),
            F.col("row_number"),
            F.col("trip_id"),
            F.col("region"),
            F.col("origin_lon"),
            F.col("origin_lat"),
            F.col("destination_lon"),
            F.col("destination_lat"),
            F.col("datetime"),
            F.col("datasource"),
            F.col("raw_payload"),
            F.col("kafka_timestamp"),
        )
    )


def _bronze_silver_foreach_batch(
    batch_df: Any,
    epoch_id: int,
    delta_base: str,
    checkpoint_base: str,
) -> None:
    """
    Process one micro-batch: Bronze (all), Silver trips (valid only), markers, rejected.
    Emit BRONZE_COMPLETED and SILVER_COMPLETED only after sink commits succeed.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession

    # 1. Bronze: append all events (trips + markers) with raw payload
    bronze_path = f"{delta_base}/bronze_trips"
    bronze_df = batch_df.withColumn("raw_payload", F.col("value").cast("string")).withColumn(
        "kafka_timestamp", F.col("timestamp")
    ).drop("value", "timestamp")
    bronze_df.write.format("delta").mode("append").save(bronze_path)

    markers_df = batch_df.filter(_is_marker())
    trips_df = batch_df.filter(_is_trip())
    valid_trips = trips_df.filter(_is_valid_trip())
    rejected = trips_df.filter(~_is_valid_trip())

    # 2. Emit BRONZE_COMPLETED for each marker (after Bronze append succeeded)
    for row in markers_df.collect():
        publish_ingestion_status(
            row["ingestion_id"],
            "BRONZE_COMPLETED",
            {"total_rows": row["total_rows"]},
        )

    # 3. Silver trips: merge with dedup by trip_id
    silver_path = f"{delta_base}/silver_trips"
    if not valid_trips.isEmpty():
        silver_trips_df = _build_silver_trips_df(valid_trips)
        if DeltaTable.isDeltaTable(spark, silver_path):
            delta_table = DeltaTable.forPath(spark, silver_path)
            (
                delta_table.alias("target")
                .merge(
                    silver_trips_df.alias("source"),
                    "target.trip_id = source.trip_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            silver_trips_df.write.format("delta").mode("append").save(silver_path)

    # 4. Silver ingestion markers
    markers_table_path = f"{delta_base}/silver_ingestion_markers"
    if not markers_df.isEmpty():
        markers_to_write = markers_df.select(
            F.col("ingestion_id"),
            F.col("control_id"),
            F.col("total_rows"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        markers_to_write.write.format("delta").mode("append").save(markers_table_path)

    # 5. Emit SILVER_COMPLETED for each marker (after Silver + markers writes succeeded)
    for row in markers_df.collect():
        publish_ingestion_status(
            row["ingestion_id"],
            "SILVER_COMPLETED",
            {"total_rows": row["total_rows"]},
        )

    # 6. Rejected trips
    if not rejected.isEmpty():
        rejected_path = f"{delta_base}/rejected_trips"
        rejected_with_meta = (
            rejected.withColumn("raw_payload", F.col("value").cast("string"))
            .withColumn("kafka_timestamp", F.col("timestamp"))
            .drop("value", "timestamp")
        )
        rejected_with_meta.write.format("delta").mode("append").save(rejected_path)


def run_job1() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    trips_topic = os.getenv("TRIPS_TOPIC", "trips.raw")
    delta_base = os.getenv("DELTA_BASE_PATH", "data/delta")
    checkpoint_base = os.getenv("CHECKPOINT_BASE_PATH", "data/checkpoints")
    max_offsets = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10"))

    spark = build_spark("job1-bronze-silver")

    # Silver schema for empty-table bootstrap (Job 2 needs it to start)
    silver_schema = StructType(
        [
            StructField("ingestion_id", StringType(), True),
            StructField("row_number", IntegerType(), True),
            StructField("trip_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("origin_lon", DoubleType(), True),
            StructField("origin_lat", DoubleType(), True),
            StructField("destination_lon", DoubleType(), True),
            StructField("destination_lat", DoubleType(), True),
            StructField("datetime", StringType(), True),
            StructField("datasource", StringType(), True),
            StructField("raw_payload", StringType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
        ]
    )
    (
        spark.createDataFrame([], silver_schema)
        .write.format("delta")
        .mode("ignore")
        .save(f"{delta_base}/silver_trips")
    )

    # Markers table bootstrap
    markers_schema = StructType(
        [
            StructField("ingestion_id", StringType(), True),
            StructField("control_id", StringType(), True),
            StructField("total_rows", IntegerType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
        ]
    )
    (
        spark.createDataFrame([], markers_schema)
        .write.format("delta")
        .mode("ignore")
        .save(f"{delta_base}/silver_ingestion_markers")
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", trips_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    parsed = raw_stream.withColumn(
        "payload",
        F.from_json(F.col("value").cast("string"), _FLEXIBLE_SCHEMA),
    ).withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

    # Flatten payload for processing; keep value for raw_payload
    flattened = parsed.select(
        F.col("payload.record_type"),
        F.col("payload.ingestion_id"),
        F.col("payload.control_id"),
        F.col("payload.total_rows"),
        F.col("payload.row_number"),
        F.col("payload.trip_id"),
        F.col("payload.region"),
        F.col("payload.origin_lon"),
        F.col("payload.origin_lat"),
        F.col("payload.destination_lon"),
        F.col("payload.destination_lat"),
        F.col("payload.datetime"),
        F.col("payload.datasource"),
        F.col("value"),
        F.col("timestamp"),
    )

    (
        flattened.writeStream.foreachBatch(
            lambda df, eid: _bronze_silver_foreach_batch(
                df, eid, delta_base, checkpoint_base
            )
        )
        .option("checkpointLocation", f"{checkpoint_base}/job1_unified")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_job1()
