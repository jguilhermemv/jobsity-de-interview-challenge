CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS regions (
    region_id SERIAL PRIMARY KEY,
    region_name TEXT UNIQUE NOT NULL,
    region_geom GEOMETRY(POLYGON, 4326)
);

CREATE TABLE IF NOT EXISTS datasources (
    datasource_id SERIAL PRIMARY KEY,
    datasource_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id TEXT PRIMARY KEY,
    region_id INTEGER REFERENCES regions(region_id),
    origin_geom GEOMETRY(POINT, 4326) NOT NULL,
    destination_geom GEOMETRY(POINT, 4326) NOT NULL,
    origin_geohash TEXT,
    destination_geohash TEXT,
    datetime TIMESTAMPTZ NOT NULL,
    datasource_id INTEGER REFERENCES datasources(datasource_id),
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trip_clusters (
    cluster_id SERIAL PRIMARY KEY,
    origin_cell TEXT NOT NULL,
    destination_cell TEXT NOT NULL,
    time_bucket TIMESTAMPTZ NOT NULL,
    trip_count BIGINT NOT NULL,
    iso_week INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trips_trip_id ON trips(trip_id);
CREATE INDEX IF NOT EXISTS idx_trips_origin_geom ON trips USING GIST(origin_geom);
CREATE INDEX IF NOT EXISTS idx_trips_destination_geom ON trips USING GIST(destination_geom);
CREATE INDEX IF NOT EXISTS idx_trip_clusters_origin_cell ON trip_clusters(origin_cell);
CREATE INDEX IF NOT EXISTS idx_trip_clusters_destination_cell ON trip_clusters(destination_cell);
