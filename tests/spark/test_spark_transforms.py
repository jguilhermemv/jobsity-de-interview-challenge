import pytest
from pyspark.sql import SparkSession

from de_challenge.spark.transforms import add_geohashes, add_time_bucket


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

    spark.stop()
