# Project Instructions (Data Engineering Challenge)

These instructions apply to the entire repository.

## Goal
Build a local, containerized solution that demonstrates scalable data engineering practices with Kafka and Spark, and can be explained as cloud-ready (AWS + Databricks).

## Reference Documents (keep updated)
- Challenge context: `docs/de-challenge-context.md`
- Exploratory analysis: `docs/analysis-trips-gpt-en-us.md`
- Solution proposal: `docs/solution-architecture-proposal-en-us.md`
- Source data: `sample_data/trips.csv`

Update these documents whenever you make relevant changes or new decisions that affect the scope, analysis, or architecture.

## Required Documentation
- Create a `README.md` that explains the problem, the solution, and how to run locally (setup + commands).

## Architecture (Local)
- REST API (FastAPI) receives CSV uploads and publishes one Kafka event per row.
- Spark Structured Streaming Job 1 reads `trips.raw`, writes Bronze (raw) and Silver (clean) Delta tables.
- Spark Structured Streaming Job 2 reads Silver, produces Gold aggregates and writes to PostgreSQL + PostGIS.
- Airflow orchestrates and monitors the two streaming jobs (long-running).
- API exposes ingestion status via SSE/WebSocket without polling.

## Architecture (Cloud Reference)
- Kafka: AWS MSK.
- Processing: Databricks on AWS (Structured Streaming + Delta Lake).
- API: FastAPI on EKS behind AWS Load Balancer.
- Database: Amazon RDS PostgreSQL with PostGIS.
- Orchestration: Databricks Workflows (MWAA optional).
- Databricks benefits: Unity Catalog, Delta Live Tables, auto-scaling, integrated observability.

## Data Contract
- Each CSV line becomes one Kafka event in `trips.raw`.
- Event fields must include `ingestion_id`, `row_number`, `trip_id`, `region`, `origin_lon`, `origin_lat`, `destination_lon`, `destination_lat`, `datetime`, `datasource`.
- `trip_id` must be deterministic and exclude `ingestion_id` to enable deduplication across batches.

## Data Modeling
- Use PostGIS geometry columns for origin and destination points.
- Use geohash/H3 (or explicit distance) for spatial similarity grouping.
- Use time buckets (e.g., 30–60 min) for temporal similarity grouping.
- Maintain a `trip_clusters` (Gold) table for fast aggregated queries.

## Deduplication and Error Handling
- Deduplicate in Spark Silver by `trip_id` with watermarking.
- Send invalid rows to a DLQ topic or a rejected table for traceability.
- Enforce unique constraint on `trip_id` in Postgres.

## Engineering Practices
- KISS, DRY, Clean Code, Clean Architecture.
- Apply DDD only where it adds clarity (Trip, Ingestion, Region, Datasource).
- Keep modules small and focused.

## Development Workflow (mandatory, cyclical)
1. Plan: create or update a plan file (e.g., `plan.md`) with clear steps.
2. Implement using **TDD**: write tests first, then implement against the plan.
3. Validate: create a results file (e.g., `results.md`) describing outcomes and how to verify them.

## Commit Message Rules
- All commit messages must be in **English**.
- Use a consistent conventional format: `<type>: <short summary>`.
- Suggested types: `feat`, `fix`, `chore`, `docs`, `test`, `refactor`.
- Examples: `feat: add Kafka ingestion endpoint`, `fix: handle null coordinates in parser`, `docs: update architecture proposal`, `test: add unit tests for deduplication`.

## Testing
- Use `pytest` for unit tests (parsing, validation, dedup rules).
- Add contract tests for Kafka event schema.
- Add light integration tests for PostGIS queries.
- Add smoke tests for streaming jobs with a micro-batch.

## Observability
- Structured JSON logs with `ingestion_id`, `trip_id`, `job_id`, `trace_id`.
- Distributed tracing with OpenTelemetry.
- Metrics for throughput, latency, error rates, Kafka lag.
- Alerts for job failure and SLA breaches.

## Tooling
- Python dependency management with Poetry.
- Formatting with `black` and `isort`.
- Linting with `ruff`.
- Type checking with `mypy` on domain and parsing modules.
- Use Docker Compose for local execution.

## Non-goals
- Do not build a GUI.
- Avoid unnecessary complexity or additional services not required by the challenge.
