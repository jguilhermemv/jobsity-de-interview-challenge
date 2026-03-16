-- Migration: add ingestion_id to trips for per-ingestion completion checks
-- Run manually if upgrading from a schema without ingestion_id:
--   psql -U trips -d trips -f sql/migrations/001_add_ingestion_id_to_trips.sql

ALTER TABLE trips ADD COLUMN IF NOT EXISTS ingestion_id TEXT;
CREATE INDEX IF NOT EXISTS idx_trips_ingestion_id ON trips(ingestion_id);
