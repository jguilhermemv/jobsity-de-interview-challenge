import os

import psycopg2
import pytest


@pytest.mark.integration
def test_postgis_schema_and_geometry():
    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        pytest.skip("POSTGRES_DSN not set")

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            with open("sql/init.sql", "r", encoding="utf-8") as handle:
                cur.execute(handle.read())

            # Verify regions lookup table accepts upserts (region_geom is nullable)
            cur.execute(
                """
                INSERT INTO regions (region_name)
                VALUES ('Test')
                ON CONFLICT (region_name) DO NOTHING;
                """
            )
            cur.execute("SELECT COUNT(*) FROM regions WHERE region_name = 'Test';")
            assert cur.fetchone()[0] == 1

            # Verify trips flat schema and geometry auto-population trigger
            cur.execute(
                """
                INSERT INTO trips (
                    trip_id, region,
                    origin_lon, origin_lat,
                    destination_lon, destination_lat,
                    datetime, datasource
                )
                VALUES (
                    'test-trip-001', 'Test',
                    14.4973794438195, 50.00136875782316,
                    14.43109483523328, 50.04052930943246,
                    '2018-05-28 09:03:40'::TIMESTAMPTZ, 'funny_car'
                )
                ON CONFLICT (trip_id) DO NOTHING;
                """
            )

            # Geometry columns must be auto-populated by the trigger
            cur.execute(
                """
                SELECT
                    ST_AsText(origin_geom),
                    ST_AsText(destination_geom),
                    region,
                    datasource
                FROM trips
                WHERE trip_id = 'test-trip-001';
                """
            )
            row = cur.fetchone()
            assert row is not None, "Trip row not found"
            assert row[0] is not None, "origin_geom not populated by trigger"
            assert row[1] is not None, "destination_geom not populated by trigger"
            assert row[2] == "Test"
            assert row[3] == "funny_car"

            # Trigger must have synced region and datasource to lookup tables
            cur.execute(
                "SELECT COUNT(*) FROM regions WHERE region_name = 'Test';"
            )
            assert cur.fetchone()[0] == 1

            cur.execute(
                "SELECT COUNT(*) FROM datasources WHERE datasource_name = 'funny_car';"
            )
            assert cur.fetchone()[0] == 1

            # Bonus query a): two most frequent regions → latest datasource
            cur.execute(
                """
                WITH top_regions AS (
                    SELECT region, COUNT(*) AS trips
                    FROM trips
                    GROUP BY region
                    ORDER BY trips DESC
                    LIMIT 2
                ), ranked AS (
                    SELECT
                        t.region,
                        t.datasource,
                        t.datetime,
                        ROW_NUMBER() OVER (
                            PARTITION BY t.region ORDER BY t.datetime DESC
                        ) AS rn
                    FROM trips t
                    JOIN top_regions tr ON tr.region = t.region
                )
                SELECT region, datasource, datetime
                FROM ranked
                WHERE rn = 1;
                """
            )
            rows = cur.fetchall()
            assert len(rows) >= 1, "Bonus query a) returned no rows"

            # Bonus query b): regions where cheap_mobile appeared
            cur.execute(
                "SELECT DISTINCT region FROM trips WHERE datasource = 'funny_car';"
            )
            regions = [r[0] for r in cur.fetchall()]
            assert "Test" in regions
