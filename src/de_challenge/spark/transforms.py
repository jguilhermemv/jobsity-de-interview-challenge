from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import geohash2


def _geohash_udf(precision: int):
    def _encode(lon: float, lat: float) -> str:
        return geohash2.encode(lat, lon, precision=precision)

    return F.udf(_encode, StringType())


def add_geohashes(df: DataFrame, precision: int) -> DataFrame:
    encode = _geohash_udf(precision)
    return (
        df.withColumn("origin_geohash", encode(F.col("origin_lon"), F.col("origin_lat")))
        .withColumn(
            "destination_geohash", encode(F.col("destination_lon"), F.col("destination_lat"))
        )
    )


def add_time_bucket(df: DataFrame, minutes: int) -> DataFrame:
    bucket_seconds = minutes * 60
    ts = F.to_timestamp("datetime")
    return df.withColumn(
        "time_bucket",
        F.from_unixtime((F.unix_timestamp(ts) / bucket_seconds).cast("long") * bucket_seconds),
    )
