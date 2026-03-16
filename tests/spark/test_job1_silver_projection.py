from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from de_challenge.spark.job1 import _build_silver_trips_df  # noqa: E402


@pytest.mark.integration
def test_build_silver_trips_df_keeps_raw_payload_and_timestamp() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-job1-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    df = spark.createDataFrame(
        [
            {
                "ingestion_id": "ing-1",
                "row_number": 1,
                "trip_id": "trip-1",
                "region": "Prague",
                "origin_lon": 14.49,
                "origin_lat": 50.00,
                "destination_lon": 14.43,
                "destination_lat": 50.04,
                "datetime": "2018-05-28T09:03:40Z",
                "datasource": "source-a",
                "value": '{"trip_id":"trip-1"}',
                "timestamp": datetime(2026, 3, 16, 21, 10, tzinfo=timezone.utc),
            }
        ]
    )

    expected_timestamp = df.select("timestamp").collect()[0].timestamp
    projected_df = _build_silver_trips_df(df)
    result = projected_df.collect()[0]

    assert result.trip_id == "trip-1"
    assert result.raw_payload == '{"trip_id":"trip-1"}'
    assert result.kafka_timestamp == expected_timestamp
    assert "value" not in projected_df.columns
    assert "timestamp" not in projected_df.columns

    spark.stop()
