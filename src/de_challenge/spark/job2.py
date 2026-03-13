from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from de_challenge.spark.transforms import add_geohashes, add_time_bucket


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


def _write_trip_clusters(df, url: str, user: str, password: str) -> None:
    (
        df.write.mode("append")
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "trip_clusters")
        .option("user", user)
        .option("password", password)
        .save()
    )


def run_job2() -> None:
    delta_base = os.getenv("DELTA_BASE_PATH", "data/delta")
    checkpoint_base = os.getenv("CHECKPOINT_BASE_PATH", "data/checkpoints")

    postgres_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/trips")
    postgres_user = os.getenv("POSTGRES_USER", "trips")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "trips")

    spark = build_spark("job2-gold-postgis")

    silver = spark.readStream.format("delta").load(f"{delta_base}/silver_trips")

    enriched = add_geohashes(silver, precision=7)
    enriched = add_time_bucket(enriched, minutes=30)

    clusters = (
        enriched.groupBy("origin_geohash", "destination_geohash", "time_bucket")
        .agg(F.count("trip_id").alias("trip_count"))
        .withColumn("iso_week", F.weekofyear("time_bucket"))
        .withColumnRenamed("origin_geohash", "origin_cell")
        .withColumnRenamed("destination_geohash", "destination_cell")
    )

    weekly_metrics = (
        enriched.withColumn("iso_week", F.weekofyear(F.to_timestamp("datetime")))
        .groupBy("region", "iso_week")
        .agg(F.count("trip_id").alias("trip_count"))
        .withColumn("avg_trips_per_day", F.col("trip_count") / F.lit(7.0))
    )

    (
        clusters.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/gold_trip_clusters")
        .outputMode("complete")
        .start(f"{delta_base}/gold_trip_clusters")
    )

    (
        weekly_metrics.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/gold_weekly_metrics")
        .outputMode("complete")
        .start(f"{delta_base}/gold_weekly_metrics")
    )

    (
        clusters.writeStream.foreachBatch(
            lambda df, _: _write_trip_clusters(df, postgres_url, postgres_user, postgres_password)
        )
        .option("checkpointLocation", f"{checkpoint_base}/postgres_trip_clusters")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_job2()
