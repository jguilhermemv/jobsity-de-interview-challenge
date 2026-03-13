# Commit-Ready Delivery Plan

## Summary
Build a local, containerized Kafka + Spark + PostGIS stack with geohash-based spatial grouping and SSE ingestion status. Each deliverable is atomic and committable, with tests and docs updated alongside the implementation.

## Interfaces
- API endpoints: `POST /ingestions`, `GET /ingestions/{ingestion_id}/events` (SSE), `GET /healthz`.
- Kafka topics: `trips.raw`, `ingestion.status`, `trips.rejected`.
- Delta tables: `bronze_trips`, `silver_trips`, `rejected_trips`, `gold_trip_clusters`, `gold_weekly_metrics`.
- Postgres tables: `trips`, `regions`, `datasources`, `trip_clusters` with PostGIS geometry columns.

## Delivery Sequence (Atomic Commits)
1. Add this plan and record chosen defaults (geohash, SSE, time bucket) aligned with the architecture doc.
2. Scaffolding and tooling: `README.md`, Poetry `pyproject.toml`, package layout, `ruff/black/isort/mypy` configs; update the architecture doc with finalized choices.
3. Docker Compose baseline: services for `api`, `kafka` (KRaft), `spark` (master + worker), `airflow` (webserver + scheduler), `postgres` (PostGIS), plus `.env.example` and README notes.
4. Domain + contract layer: models, deterministic `trip_id` hash (excluding `ingestion_id`), Kafka event schema module, and contract tests.
5. API ingestion: FastAPI `POST /ingestions` to stream CSV rows and emit one Kafka event per row; unit tests for parsing/validation.
6. Status streaming: `ingestion.status` publisher hooks and SSE endpoint; tests for SSE formatting and Kafka consumer behavior.
7. Spark Job 1: Kafka read, validation, watermark + dedup by `trip_id`, write Bronze/Silver Delta, write invalid rows to `rejected_trips` and `trips.rejected`, with unit tests.
8. Postgres schema + PostGIS: SQL init/migrations, geometry columns, unique constraint on `trip_id`, and a light integration test.
9. Spark Job 2: read Silver, compute geohash cells + 30-minute buckets, aggregate `trip_clusters` + weekly metrics, write to Postgres, plus a micro-batch smoke test.
10. Airflow orchestration: DAG to start both Spark streaming jobs via `spark-submit`, with restart-on-failure and logging; document run/stop.
11. Observability: structured JSON logs with `ingestion_id`, `trip_id`, `job_id`, `trace_id`, and minimal OpenTelemetry wiring; document conventions.
12. `results.md`: document outcomes, how to run locally, verification steps; update analysis/architecture docs if implementation diverged.

## Defaults
- Geospatial grouping uses geohash precision 7.
- Time bucket is 30 minutes.
- SSE is the only status channel (no WebSocket).
- Invalid rows are written to both `rejected_trips` Delta and `trips.rejected` Kafka topic.
- Delta tables stored under `data/delta/`.
- Postgres is the system of record for gold tables with unique constraint on `trip_id`.
