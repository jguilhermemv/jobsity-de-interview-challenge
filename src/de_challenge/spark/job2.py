from __future__ import annotations

import logging
import os
import time
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

from de_challenge.spark.status_publisher import publish_ingestion_status
from de_challenge.spark.transforms import add_geohashes, add_time_bucket

logger = logging.getLogger(__name__)

# Completion check: retry until PG count reaches total_rows
_COMPLETION_MAX_WAIT = int(os.getenv("TRIPS_PG_COMPLETION_MAX_WAIT", "60"))
_COMPLETION_POLL_INTERVAL = float(os.getenv("TRIPS_PG_COMPLETION_POLL_INTERVAL", "2.0"))

# Retry config for transient PostgreSQL/network failures
_WRITE_MAX_RETRIES = int(os.getenv("POSTGRES_WRITE_MAX_RETRIES", "3"))
_WRITE_INITIAL_BACKOFF = float(os.getenv("POSTGRES_WRITE_INITIAL_BACKOFF", "2.0"))
_CONNECT_TIMEOUT = int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "15"))

_TRIPS_COLS = [
    "trip_id",
    "ingestion_id",
    "region",
    "origin_lon",
    "origin_lat",
    "destination_lon",
    "destination_lat",
    "origin_geohash",
    "destination_geohash",
    "datetime",
    "datasource",
]


def _wait_for_silver(silver_path: str, timeout: int = 600, interval: int = 3) -> None:
    """
    Block until the Silver Delta table has at least one committed transaction.

    Job1 (bronze→silver) and Job2 (silver→gold) start in parallel. On a fresh
    environment the Silver table may not exist yet; this guard prevents Job2
    from crashing with DELTA_SCHEMA_NOT_SET before Job1 writes the first batch.
    """
    log_dir = os.path.join(silver_path, "_delta_log")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.isdir(log_dir) and any(
            f.endswith(".json") for f in os.listdir(log_dir)
        ):
            print(f"Silver table ready at {silver_path}")
            return
        print(f"Silver table not ready yet, retrying in {interval}s…")
        time.sleep(interval)
    raise TimeoutError(
        f"Silver Delta table did not appear at {silver_path} within {timeout}s"
    )


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)  # pyright: ignore[reportAttributeAccessIssue]
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def _write_trip_clusters(df: DataFrame, url: str, user: str, password: str) -> None:
    """Write aggregated clusters to PostgreSQL via JDBC with retry for transient failures."""
    writer = (
        df.write.mode("append")
        .format("jdbc")
        .option("url", url)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "trip_clusters")
        .option("user", user)
        .option("password", password)
        .option("connectTimeout", str(_CONNECT_TIMEOUT))
        .option("socketTimeout", str(_CONNECT_TIMEOUT * 2))
    )
    last_err = None
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            writer.save()
            return
        except Exception as e:
            last_err = e
            if attempt < _WRITE_MAX_RETRIES - 1:
                backoff = _WRITE_INITIAL_BACKOFF * (2**attempt)
                logger.warning(
                    "JDBC write to trip_clusters failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _WRITE_MAX_RETRIES,
                    e,
                    backoff,
                )
                time.sleep(backoff)
            else:
                raise last_err


def _process_markers_batch(
    batch_df: "DataFrame",
    epoch_id: int,
    jdbc_url: str,
    user: str,
    password: str,
) -> None:
    """
    For each ingestion-end marker: verify PostgreSQL has total_rows for that
    ingestion_id, then emit TRIPS_PG_COMPLETED. Retries briefly until count matches.
    """
    import psycopg2

    rows = batch_df.select("ingestion_id", "total_rows").collect()
    if not rows:
        return

    plain = jdbc_url[len("jdbc:"):]
    parsed = urlparse(plain)
    host = parsed.hostname
    port = parsed.port or 5432
    database = parsed.path.lstrip("/")

    for row in rows:
        ingestion_id = row["ingestion_id"]
        total_rows = int(row["total_rows"])
        deadline = time.time() + _COMPLETION_MAX_WAIT
        count = 0
        emitted = False
        while time.time() < deadline:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                    connect_timeout=_CONNECT_TIMEOUT,
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM trips WHERE ingestion_id = %s",
                            (ingestion_id,),
                        )
                        row = cur.fetchone()
                        count = row[0] if row else 0
                    if count >= total_rows:
                        publish_ingestion_status(
                            ingestion_id,
                            "TRIPS_PG_COMPLETED",
                            {"total_rows": total_rows, "pg_count": count},
                        )
                        emitted = True
                        break
                finally:
                    conn.close()
            except Exception as e:
                logger.warning(
                    "TRIPS_PG completion check failed for %s: %s",
                    ingestion_id,
                    e,
                )
            time.sleep(_COMPLETION_POLL_INTERVAL)
        if not emitted:
            logger.warning(
                "TRIPS_PG_COMPLETED not emitted: ingestion %s count %d < %d after %ds",
                ingestion_id,
                count,
                total_rows,
                _COMPLETION_MAX_WAIT,
            )


def _write_trips_batch(
    df: DataFrame,
    epoch_id: int,
    jdbc_url: str,
    user: str,
    password: str,
) -> None:
    """
    Write a micro-batch of silver trips into the PostgreSQL trips table.

    Uses psycopg2 directly instead of Spark JDBC's preActions/postActions
    mechanism, which does not reliably execute postActions in streaming
    foreachBatch contexts. psycopg2 is available in the Airflow driver
    container (installed via airflow.Dockerfile).

    The INSERT uses ON CONFLICT (trip_id) DO NOTHING to make each micro-batch
    write idempotent — safe to replay after a Spark restart.
    Geohashes are computed here because the incoming silver DataFrame does not
    yet carry geohash columns.
    """
    import psycopg2
    import psycopg2.extras

    enriched = add_geohashes(df, precision=7)
    rows = enriched.select(*_TRIPS_COLS).collect()
    if not rows:
        return

    # Parse jdbc:postgresql://host:port/db into psycopg2 connect args
    plain = jdbc_url[len("jdbc:"):]  # strip leading "jdbc:" scheme
    parsed = urlparse(plain)
    host = parsed.hostname
    port = parsed.port or 5432
    database = parsed.path.lstrip("/")

    last_err = None
    for attempt in range(_WRITE_MAX_RETRIES):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=_CONNECT_TIMEOUT,
            )
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO trips (
                            trip_id, ingestion_id, region,
                            origin_lon, origin_lat,
                            destination_lon, destination_lat,
                            origin_geohash, destination_geohash,
                            datetime, datasource
                        )
                        VALUES %s
                        ON CONFLICT (trip_id) DO NOTHING
                        """,
                        [
                            (
                                r.trip_id,
                                r.ingestion_id,
                                r.region,
                                float(r.origin_lon),
                                float(r.origin_lat),
                                float(r.destination_lon),
                                float(r.destination_lat),
                                r.origin_geohash,
                                r.destination_geohash,
                                r.datetime,
                                r.datasource,
                            )
                            for r in rows
                        ],
                    )
                conn.commit()
            finally:
                conn.close()
            return
        except Exception as e:
            last_err = e
            if attempt < _WRITE_MAX_RETRIES - 1:
                backoff = _WRITE_INITIAL_BACKOFF * (2**attempt)
                logger.warning(
                    "psycopg2 write to trips failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1,
                    _WRITE_MAX_RETRIES,
                    e,
                    backoff,
                )
                time.sleep(backoff)
            else:
                raise last_err


def run_job2() -> None:
    delta_base = os.getenv("DELTA_BASE_PATH", "data/delta")
    checkpoint_base = os.getenv("CHECKPOINT_BASE_PATH", "data/checkpoints")

    postgres_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/trips")
    postgres_user = os.getenv("POSTGRES_USER", "trips")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "trips")
    max_files = int(os.getenv("MAX_FILES_PER_TRIGGER", "1"))

    spark = build_spark("job2-gold-postgis")

    _wait_for_silver(f"{delta_base}/silver_trips")
    _wait_for_silver(f"{delta_base}/silver_ingestion_markers")
    silver = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files)
        .load(f"{delta_base}/silver_trips")
    )

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

    # --- Stream 1: Silver → Delta Gold (trip clusters) ---
    (
        clusters.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/gold_trip_clusters")
        .outputMode("complete")
        .trigger(processingTime="5 seconds")
        .start(f"{delta_base}/gold_trip_clusters")
    )

    # --- Stream 2: Silver → Delta Gold (weekly metrics) ---
    (
        weekly_metrics.writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/gold_weekly_metrics")
        .outputMode("complete")
        .trigger(processingTime="5 seconds")
        .start(f"{delta_base}/gold_weekly_metrics")
    )

    # --- Stream 3: Silver → PostgreSQL trips (individual rows, idempotent) ---
    (
        silver.writeStream.foreachBatch(
            lambda df, eid: _write_trips_batch(
                df, eid, postgres_url, postgres_user, postgres_password
            )
        )
        .option("checkpointLocation", f"{checkpoint_base}/postgres_trips")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # --- Stream 4: Gold clusters → PostgreSQL trip_clusters (aggregates) ---
    (
        clusters.writeStream.foreachBatch(
            lambda df, _: _write_trip_clusters(
                df, postgres_url, postgres_user, postgres_password
            )
        )
        .option("checkpointLocation", f"{checkpoint_base}/postgres_trip_clusters")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # --- Stream 5: Markers → verify PG trips count, emit TRIPS_PG_COMPLETED ---
    markers_stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files)
        .load(f"{delta_base}/silver_ingestion_markers")
    )
    (
        markers_stream.writeStream.foreachBatch(
            lambda df, eid: _process_markers_batch(
                df, eid, postgres_url, postgres_user, postgres_password
            )
        )
        .option("checkpointLocation", f"{checkpoint_base}/markers_completion")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_job2()
