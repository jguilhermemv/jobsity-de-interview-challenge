from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras


def _conn_params() -> dict[str, object]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "trips"),
        "user": os.getenv("POSTGRES_USER", "trips"),
        "password": os.getenv("POSTGRES_PASSWORD", "trips"),
    }


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(**_conn_params())
    try:
        yield conn
    finally:
        conn.close()


def weekly_average_by_region(region: str) -> dict[str, object]:
    """Return the weekly average trip count for a named region."""
    sql = """
        WITH weekly AS (
            SELECT DATE_TRUNC('week', datetime) AS week_start,
                   COUNT(*) AS trip_count
            FROM trips
            WHERE region = %(region)s
            GROUP BY week_start
        )
        SELECT COALESCE(AVG(trip_count), 0)  AS weekly_average,
               COUNT(*)                       AS num_weeks,
               COALESCE(SUM(trip_count), 0)  AS total_trips
        FROM weekly
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, {"region": region})
            row = cur.fetchone()
    return dict(row)  # type: ignore[arg-type]


def weekly_average_by_bbox(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float
) -> dict[str, object]:
    """Return the weekly average trip count for trips whose origin falls inside a bounding box."""
    sql = """
        WITH weekly AS (
            SELECT DATE_TRUNC('week', datetime) AS week_start,
                   COUNT(*) AS trip_count
            FROM trips
            WHERE ST_Within(
                      origin_geom,
                      ST_MakeEnvelope(%(min_lon)s, %(min_lat)s, %(max_lon)s, %(max_lat)s, 4326)
                  )
            GROUP BY week_start
        )
        SELECT COALESCE(AVG(trip_count), 0)  AS weekly_average,
               COUNT(*)                       AS num_weeks,
               COALESCE(SUM(trip_count), 0)  AS total_trips
        FROM weekly
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql,
                {
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                },
            )
            row = cur.fetchone()
    return dict(row)  # type: ignore[arg-type]
