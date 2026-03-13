# Results

## What was implemented
- FastAPI ingestion service with CSV upload and SSE ingestion status streaming.
- Kafka topics for `trips.raw`, `ingestion.status`, and `trips.rejected`.
- Spark Structured Streaming jobs:
  - Job 1: Kafka -> Bronze/Silver/Rejected Delta tables with deduplication.
  - Job 2: Silver -> Gold aggregates and Postgres writes.
- Postgres/PostGIS schema for trips and trip clusters.
- Airflow DAG to orchestrate streaming jobs.
- Tests for parsing, validation, schema contracts, SSE formatting, and Spark transforms.

## How to run
1. Start the stack:
   ```bash
   cp .env.example .env
   docker compose up -d
   ```
2. Upload data:
   ```bash
   curl -F "file=@sample_data/trips.csv" http://localhost:8000/ingestions
   ```
3. Stream status:
   ```bash
   curl -N http://localhost:8000/ingestions/<ingestion_id>/events
   ```
4. Airflow UI:
   - `http://localhost:8081` (DAG: `streaming_jobs`)

## Verification
- Unit tests: `poetry run pytest`
- PostGIS integration (optional): set `POSTGRES_DSN` and run `poetry run pytest -m integration`
- Spark smoke test (optional): `poetry run pytest -m integration` (requires local Spark)
