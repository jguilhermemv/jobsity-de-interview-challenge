# Jobsity Data Engineering Challenge

Local, containerized data pipeline demonstrating Kafka ingestion, Spark Structured Streaming, Delta Lake storage, and PostGIS serving. The architecture mirrors a cloud-ready design (AWS + Databricks) while remaining fully runnable on a single machine.

## Architecture Summary
- **API**: FastAPI receives CSV uploads and emits one Kafka event per row (`trips.raw`).
- **Job 1**: Spark reads `trips.raw`, writes **Bronze** (raw) and **Silver** (clean) Delta tables, deduplicating by `trip_id`.
- **Job 2**: Spark reads Silver, computes **Gold** aggregates, and writes to Postgres/PostGIS.
- **Status**: ingestion status events are published to `ingestion.status` and streamed to clients via **SSE**.
- **Orchestration**: Airflow starts and monitors the two long-running Spark jobs.

## Quick Start (Local)
1. Copy env file and adjust if needed:
   ```bash
   cp .env.example .env
   ```
2. Start the stack:
   ```bash
   docker compose up -d
   ```
3. Upload data:
   ```bash
   curl -F "file=@sample_data/trips.csv" http://localhost:8000/ingestions
   ```
4. Watch ingestion status:
   ```bash
   curl -N http://localhost:8000/ingestions/<ingestion_id>/events
   ```

## Services (Docker Compose)
- `api`: FastAPI ingestion service
- `kafka`: Kafka in KRaft mode
- `spark-master` / `spark-worker`: Spark cluster
- `airflow-webserver` / `airflow-scheduler`: orchestration
- `postgres`: PostgreSQL + PostGIS

## Data Layout
- Delta Lake: `data/delta/` (local volume)
- Checkpoints: `data/checkpoints/` (local volume)
- Postgres data: `data/postgres/`

## Airflow
- UI: `http://localhost:8081`
- DAG: `streaming_jobs` runs `job1` then `job2` using `spark-submit`.

## Tests
```bash
poetry run pytest
```

## Development
- Formatting: `black`, `isort`
- Linting: `ruff`
- Type checking: `mypy`

## Notes
- Geospatial grouping uses **geohash precision 7**.
- Time buckets are **30 minutes**.
- Status channel uses **SSE** only (no WebSocket).

## Bonus Queries
See `sql/bonus_queries.sql`.
