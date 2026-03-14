CREATE EXTENSION IF NOT EXISTS postgis;

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- ---------------------------------------------------------------------------
-- Lookup tables (kept in sync automatically via trigger on trips inserts)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS regions (
    region_id   SERIAL PRIMARY KEY,
    region_name TEXT UNIQUE NOT NULL,
    region_geom GEOMETRY(POLYGON, 4326)  -- optional bounding polygon
);

CREATE TABLE IF NOT EXISTS datasources (
    datasource_id   SERIAL PRIMARY KEY,
    datasource_name TEXT UNIQUE NOT NULL
);

-- ---------------------------------------------------------------------------
-- Main trips table
--
-- Uses a *flat* schema (region TEXT, datasource TEXT, lon/lat as DOUBLE) so
-- that Spark JDBC can write directly without type-mapping issues.
-- Geometry columns (origin_geom, destination_geom) are auto-populated from
-- the lon/lat values by the trigger below — no need to write them from Spark.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS trips (
    trip_id              TEXT PRIMARY KEY,
    region               TEXT NOT NULL,
    origin_lon           DOUBLE PRECISION NOT NULL,
    origin_lat           DOUBLE PRECISION NOT NULL,
    destination_lon      DOUBLE PRECISION NOT NULL,
    destination_lat      DOUBLE PRECISION NOT NULL,
    origin_geom          GEOMETRY(POINT, 4326),    -- auto-set by trigger
    destination_geom     GEOMETRY(POINT, 4326),    -- auto-set by trigger
    origin_geohash       TEXT,
    destination_geohash  TEXT,
    datetime             TIMESTAMPTZ NOT NULL,
    datasource           TEXT NOT NULL,
    ingested_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Gold aggregation table (written by Spark Job2)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS trip_clusters (
    cluster_id        SERIAL PRIMARY KEY,
    origin_cell       TEXT NOT NULL,
    destination_cell  TEXT NOT NULL,
    time_bucket       TIMESTAMPTZ NOT NULL,
    trip_count        BIGINT NOT NULL,
    iso_week          INTEGER NOT NULL
);

-- ---------------------------------------------------------------------------
-- Trigger: auto-populate geometry from lon/lat on every INSERT or UPDATE
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION populate_trip_geometry()
RETURNS TRIGGER AS $$
BEGIN
    NEW.origin_geom      := ST_SetSRID(ST_MakePoint(NEW.origin_lon,      NEW.origin_lat),      4326);
    NEW.destination_geom := ST_SetSRID(ST_MakePoint(NEW.destination_lon, NEW.destination_lat), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trip_geometry_trigger
    BEFORE INSERT OR UPDATE ON trips
    FOR EACH ROW EXECUTE FUNCTION populate_trip_geometry();

-- ---------------------------------------------------------------------------
-- Trigger: keep regions and datasources lookup tables in sync
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION sync_lookup_tables()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO regions     (region_name)      VALUES (NEW.region)      ON CONFLICT (region_name)      DO NOTHING;
    INSERT INTO datasources (datasource_name)  VALUES (NEW.datasource)  ON CONFLICT (datasource_name) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trip_lookup_sync_trigger
    AFTER INSERT ON trips
    FOR EACH ROW EXECUTE FUNCTION sync_lookup_tables();

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS idx_trips_trip_id               ON trips(trip_id);
CREATE        INDEX IF NOT EXISTS idx_trips_region                ON trips(region);
CREATE        INDEX IF NOT EXISTS idx_trips_datasource            ON trips(datasource);
CREATE        INDEX IF NOT EXISTS idx_trips_datetime              ON trips(datetime);
CREATE        INDEX IF NOT EXISTS idx_trips_origin_geom           ON trips USING GIST(origin_geom);
CREATE        INDEX IF NOT EXISTS idx_trips_destination_geom      ON trips USING GIST(destination_geom);
CREATE INDEX IF NOT EXISTS idx_trip_clusters_origin_cell   ON trip_clusters(origin_cell);
CREATE INDEX IF NOT EXISTS idx_trip_clusters_dest_cell     ON trip_clusters(destination_cell);
