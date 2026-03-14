from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _geohash_col(lon_col, lat_col, precision: int):
    """
    Compute a geohash as a pure Spark SQL column expression.

    Implements the standard geohash algorithm entirely with native Spark
    arithmetic and string functions — no Python UDF, no executor-side Python.

    Algorithm:
      1. Normalize each coordinate to an integer bucket [0, 2^n).
      2. Extract individual bits (MSB→LSB) via floor-division and modulo.
      3. Interleave lon/lat bits (lon at even positions, lat at odd).
      4. Group every 5 bits into a base-32 character via SUBSTRING on a
         constant string.
    """
    n_bits = precision * 5
    lon_n = (n_bits + 1) // 2  # lon gets the extra bit when n_bits is odd
    lat_n = n_bits // 2

    # Normalize coordinates to [0, 2^n) integer range, clamped at boundaries.
    lon_int = F.least(
        F.lit(2**lon_n - 1).cast("long"),
        F.greatest(
            F.lit(0).cast("long"),
            ((lon_col + 180.0) / 360.0 * float(2**lon_n)).cast("long"),
        ),
    )
    lat_int = F.least(
        F.lit(2**lat_n - 1).cast("long"),
        F.greatest(
            F.lit(0).cast("long"),
            ((lat_col + 90.0) / 180.0 * float(2**lat_n)).cast("long"),
        ),
    )

    # Constant array of the 32 base-32 symbols (1-based index for element_at).
    # F.element_at accepts a Column as index — F.substring does not.
    base32_array = F.array(*[F.lit(c) for c in _BASE32])

    chars = []

    for char_idx in range(precision):
        char_val = F.lit(0).cast("long")

        for bit_in_char in range(5):
            abs_bit = char_idx * 5 + bit_in_char
            weight = 2 ** (4 - bit_in_char)

            if abs_bit % 2 == 0:
                # Longitude bit: even absolute positions (0, 2, 4, …)
                lsb_pos = lon_n - 1 - abs_bit // 2
                bit_val = F.floor(lon_int / float(2**lsb_pos)).cast("long") % 2
            else:
                # Latitude bit: odd absolute positions (1, 3, 5, …)
                lsb_pos = lat_n - 1 - (abs_bit - 1) // 2
                bit_val = F.floor(lat_int / float(2**lsb_pos)).cast("long") % 2

            char_val = char_val + bit_val * F.lit(weight).cast("long")

        # char_val is 0–31; element_at uses 1-based index → +1
        chars.append(F.element_at(base32_array, char_val.cast("int") + 1))

    return F.concat(*chars)


def add_geohashes(df: DataFrame, precision: int) -> DataFrame:
    return df.withColumn(
        "origin_geohash",
        _geohash_col(F.col("origin_lon"), F.col("origin_lat"), precision),
    ).withColumn(
        "destination_geohash",
        _geohash_col(F.col("destination_lon"), F.col("destination_lat"), precision),
    )


def add_time_bucket(df: DataFrame, minutes: int) -> DataFrame:
    bucket_seconds = minutes * 60
    ts = F.to_timestamp("datetime")
    return df.withColumn(
        "time_bucket",
        F.from_unixtime(
            (F.unix_timestamp(ts) / bucket_seconds).cast("long") * bucket_seconds
        ).cast("timestamp"),
    )
