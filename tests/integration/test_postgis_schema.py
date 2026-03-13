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

            cur.execute(
                """
                INSERT INTO regions (region_name, region_geom)
                VALUES ('Test', ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))', 4326));
                """
            )
            cur.execute("SELECT COUNT(*) FROM regions WHERE region_name = 'Test';")
            count = cur.fetchone()[0]

    assert count == 1
