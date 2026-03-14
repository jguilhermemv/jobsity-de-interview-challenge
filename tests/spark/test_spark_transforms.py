import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from de_challenge.spark.transforms import add_geohashes, add_time_bucket  # noqa: E402

_BASE32_CHARS = set("0123456789bcdefghjkmnpqrstuvwxyz")

# Known geohash values for reference points (validated against the standard
# geohash algorithm).  Precision-1 values are used because they are easy to
# verify by hand.
_KNOWN = [
    # (lon, lat, precision, expected_geohash)
    (0.0, 0.0, 1, "s"),       # equator / prime meridian → 's'
    (-0.1, 51.5, 1, "g"),     # London area → 'g'
    (2.35, 48.86, 1, "u"),    # Paris → 'u'
]


@pytest.mark.integration
def test_spark_transforms_smoke():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    data = [
        {
            "origin_lon": 14.4973794,
            "origin_lat": 50.0013687,
            "destination_lon": 14.4310948,
            "destination_lat": 50.0405293,
            "datetime": "2018-05-28T09:03:40Z",
        }
    ]
    df = spark.createDataFrame(data)
    df = add_geohashes(df, precision=7)
    df = add_time_bucket(df, minutes=30)
    row = df.collect()[0]

    assert row.origin_geohash is not None
    assert row.destination_geohash is not None
    assert row.time_bucket is not None

    assert len(row.origin_geohash) == 7
    assert len(row.destination_geohash) == 7
    assert all(c in _BASE32_CHARS for c in row.origin_geohash)
    assert all(c in _BASE32_CHARS for c in row.destination_geohash)

    spark.stop()


@pytest.mark.integration
@pytest.mark.parametrize("lon,lat,precision,expected", _KNOWN)
def test_geohash_known_values(lon, lat, precision, expected):
    """Verify that the native Spark implementation matches known geohash values."""
    spark = SparkSession.builder.master("local[1]").appName("test-geohash").getOrCreate()
    data = [{"origin_lon": lon, "origin_lat": lat,
             "destination_lon": lon, "destination_lat": lat}]
    df = spark.createDataFrame(data)
    df = add_geohashes(df, precision=precision)
    row = df.collect()[0]

    assert row.origin_geohash == expected, (
        f"geohash({lon}, {lat}, {precision}) = {row.origin_geohash!r}, "
        f"expected {expected!r}"
    )
    spark.stop()
