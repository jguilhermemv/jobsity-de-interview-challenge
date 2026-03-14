# Jobsity Data Engineering Challenge

Local, containerized data pipeline demonstrating Kafka ingestion, Spark Structured Streaming, Delta Lake storage, and PostgreSQL/PostGIS serving. The architecture mirrors a cloud-ready design (AWS + Databricks) while remaining fully runnable on a single machine.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Is It Streaming or Batch?](#is-it-streaming-or-batch)
3. [Quick Start](#quick-start)
4. [Reset and Reprocess from Scratch](#reset-and-reprocess-from-scratch)
5. [Validate Each Pipeline Stage](#validate-each-pipeline-stage)
6. [Connect to PostgreSQL via DBeaver](#connect-to-postgresql-via-dbeaver)
7. [Observability Interfaces](#observability-interfaces)
8. [Data Layout](#data-layout)
9. [Airflow](#airflow)
10. [Tests](#tests)
11. [Development](#development)
12. [Bonus Queries](#bonus-queries)

---

## Architecture

```
CSV upload
    │
    ▼
FastAPI (port 8000)
    │  publishes 1 Kafka event per row
    ▼
Kafka topic: trips.raw
    │
    ▼
Job 1 (Spark Structured Streaming) ──── Bronze Delta (raw)
    │                                ├── Silver Delta (deduped + validated)
    │                                └── Rejected Delta (invalid rows)
    ▼
Job 2 (Spark Structured Streaming) ──── Gold Delta (trip_clusters, weekly_metrics)
                                    └── PostgreSQL/PostGIS (trip_clusters) ← STREAMING
```

- **API**: FastAPI receives CSV uploads and publishes one Kafka event per row to the `trips.raw` topic.
- **Job 1**: Reads `trips.raw`, writes **Bronze** (raw) and **Silver** (clean, deduplicated by `trip_id`) Delta tables.
- **Job 2**: Reads Silver in streaming mode, computes **Gold** aggregates, and writes to PostgreSQL/PostGIS.
- **Status**: ingestion status events are published to `ingestion.status` and streamed to clients via **SSE**.
- **Orchestration**: Airflow starts and monitors the two long-running streaming jobs.

---

## Is It Streaming or Batch?

### Short answer: **Spark Structured Streaming with micro-batches**

Neither job is traditional batch. Both use the Spark Streaming API and run **continuously** until manually stopped.

### How it works internally

| Stage | Mechanism | Behavior |
|---|---|---|
| Kafka read (Job 1) | `readStream.format("kafka")` | Micro-batch every ~200ms–2s: processes only new messages since the last checkpoint |
| Bronze/Silver Delta write | `writeStream.outputMode("append")` | Each micro-batch appends a new Parquet file to Delta Lake |
| Silver read (Job 2) | `readStream.format("delta")` | Detects new Delta files and processes them in micro-batches |
| Gold Delta write | `writeStream.outputMode("complete")` | Recomputes the full aggregation and overwrites the Gold table on every micro-batch |
| **PostgreSQL write** | `writeStream.foreachBatch(...)` | **On every micro-batch**, Spark calls `_write_trip_clusters` and inserts aggregated rows into Postgres via JDBC |

### Why it is not batch

- The jobs **do not terminate** after processing data — they keep waiting for new events.
- New CSV uploads trigger new micro-batches automatically, **without restarting the job**.
- Deduplication state (watermark in Silver) is preserved across micro-batches via **checkpoints** in `data/checkpoints/`.
- The Spark UI at `http://localhost:8080` shows queries marked as `ACTIVE` while the jobs are running.

### Micro-batch ≠ record-at-a-time

Spark Structured Streaming processes data in time windows (micro-batches), not record by record. This differs from systems like Apache Flink with native continuous processing — but it is **fundamentally different from batch**: no scheduling required, already-processed data is never reprocessed, and new data is reacted to within seconds.

---

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- ~4 GB of RAM available for the containers

### 1. Set up environment variables

```bash
cp .env.example .env
```

### 2. Start the full stack

```bash
docker compose up -d
```

Wait for all services to become healthy (~30–60 seconds).

### 3. Initialize Airflow (first time only)

```bash
# Create the Airflow metadata database
docker exec -it postgres psql -U trips -d postgres -c "CREATE DATABASE airflow;"

# Initialize the Airflow schema
docker compose run --rm airflow-webserver airflow db init

# Create an admin user
docker compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

# Start Airflow
docker compose up -d airflow-webserver airflow-scheduler
```

### 4. Trigger the streaming pipeline

```bash
docker exec airflow-webserver airflow dags trigger trips_streaming_pipeline \
  --run-id "trips_streaming_$(date +%Y%m%d_%H%M)"
```

This starts **Job 1** (Kafka → Bronze/Silver) and **Job 2** (Silver → Gold → Postgres) in parallel. Both jobs run indefinitely.

### 5. Upload the CSV

```bash
curl -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions
```

Take note of the returned `ingestion_id`.

### 6. Watch ingestion status via SSE

```bash
curl -N http://localhost:8000/ingestions/<ingestion_id>/events
```

---

## Reset and Reprocess from Scratch

Use this procedure when you want to wipe all state and reprocess data as if it were the very first run.

### What needs to be cleared

| What | Where | Why |
|---|---|---|
| Delta tables | `data/delta/` | Contains all processed Bronze, Silver, and Gold data |
| Spark checkpoints | `data/checkpoints/` | Store already-processed offsets — without clearing them, Spark skips old data |
| Postgres table | `trips` database | Contains the clusters inserted by Job 2 |
| Kafka offsets | `trips.raw` topic | Events remain in Kafka; the checkpoint is what controls what has been read |

> **Warning**: if you clear checkpoints but **not** the Delta tables, Spark will reprocess Kafka events and try to write already-existing data to Delta — potentially creating duplicates in Postgres. Always clear both together.

### Full reset step by step

**1. Stop the streaming jobs (via Airflow or directly)**

```bash
# Option A: clear tasks in Airflow
docker exec airflow-webserver airflow tasks clear trips_streaming_pipeline \
  --yes --downstream --upstream

# Option B: kill Spark processes directly
docker exec spark-worker pkill -f "job1.py" || true
docker exec spark-worker pkill -f "job2.py" || true
```

**2. Clear Delta tables and checkpoints**

```bash
# Remove all Delta layers
rm -rf data/delta/bronze_trips
rm -rf data/delta/silver_trips
rm -rf data/delta/rejected_trips
rm -rf data/delta/gold_trip_clusters
rm -rf data/delta/gold_weekly_metrics

# Remove all Spark checkpoints
rm -rf data/checkpoints/
```

**3. Clear the Postgres table**

```bash
docker exec -it postgres psql -U trips -d trips -c "TRUNCATE TABLE trip_clusters;"
```

**4. (Optional) Recreate the Kafka topic to reset offsets**

If you also want to remove old Kafka messages:

```bash
# Delete the topic
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic trips.raw

# The topic is automatically recreated on the next upload (auto.create.topics.enable=true)
```

> If you do not delete the topic, old events remain in Kafka. Because the checkpoints were cleared, Job 1 will reprocess **all** events from the beginning (`startingOffsets=earliest`). This is the correct behavior for a full reprocess.

**5. Restart the streaming jobs**

```bash
docker exec airflow-webserver airflow dags trigger trips_streaming_pipeline \
  --run-id "trips_streaming_reset_$(date +%Y%m%d_%H%M)"
```

**6. Upload the data again**

```bash
curl -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions
```

---

## Validate Each Pipeline Stage

This section shows how to verify, stage by stage, that data is flowing correctly: from the API all the way to PostgreSQL.

---

### Stage 1 — CSV successfully sent to the API

**What to validate**: the API received the file and published the events to Kafka.

```bash
curl -s -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions
```

**Expected response** (HTTP 202):

```json
{
  "ingestion_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "accepted",
  "row_count": 200
}
```

If `row_count > 0` and `status: accepted`, the API parsed the CSV and published the events.

**Follow real-time status via SSE**:

```bash
curl -N http://localhost:8000/ingestions/<ingestion_id>/events
```

Expected output:

```
data: {"ingestion_id": "...", "status": "processing", "processed": 50}
data: {"ingestion_id": "...", "status": "processing", "processed": 150}
data: {"ingestion_id": "...", "status": "done", "processed": 200}
```

---

### Stage 2 — Events arriving in Kafka

**What to validate**: events are present in the `trips.raw` topic.

```bash
# Check the current offset (should equal the number of CSV rows sent)
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic trips.raw \
  --time -1
```

**Expected result**: the offset should match the number of rows uploaded (e.g., `trips.raw:0:200`).

**Read the first few messages from the topic**:

```bash
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trips.raw \
  --from-beginning \
  --max-messages 3
```

**Expected output**: one JSON event per line:

```json
{"ingestion_id":"abc...","row_number":1,"trip_id":"d4e5...","region":"Prague","origin_lon":14.4973,"origin_lat":50.0755,...}
{"ingestion_id":"abc...","row_number":2,"trip_id":"f6a7...","region":"Turin",...}
```

---

### Stage 3 — Job 1 processing (Bronze and Silver in Delta)

**What to validate**: Spark read the Kafka events and created the Delta files.

**3a. Check Delta files on disk**:

```bash
# Bronze should contain Parquet files
ls data/delta/bronze_trips/

# Silver (valid and deduplicated data)
ls data/delta/silver_trips/

# Rejected (invalid rows, if any)
ls data/delta/rejected_trips/
```

**Expected result**: each directory contains a `_delta_log/` folder with `.json` files and subdirectories with `.parquet` files.

**3b. Check the checkpoints**:

```bash
ls data/checkpoints/bronze_trips/
ls data/checkpoints/silver_trips/
```

If the directories exist and contain files, Job 1 is (or was) processing.

**3c. Check the Spark UI**:

Go to `http://localhost:8080` → click the Application ID for **job1-bronze-silver** → **Structured Streaming** tab.

You will see 3 active queries (bronze, silver, rejected) with:

- **Status**: `ACTIVE`
- **Batch ID**: a number that increases with every micro-batch
- **Input rows/sec**: current processing rate

---

### Stage 4 — Job 2 processing (Gold and Postgres)

**What to validate**: Spark read Silver, computed the aggregates, and inserted data into Postgres.

**4a. Check Gold Delta files**:

```bash
ls data/delta/gold_trip_clusters/
ls data/delta/gold_weekly_metrics/
```

**4b. Check the Postgres checkpoint**:

```bash
ls data/checkpoints/postgres_trip_clusters/
```

If this directory exists, Job 2 attempted (or is attempting) to write to Postgres.

**4c. Check the Spark UI**:

Go to `http://localhost:8080` → click the Application ID for **job2-gold-postgis** → **Structured Streaming** tab.

You will see queries with status `ACTIVE` and a **Batch ID** that keeps incrementing.

---

### Stage 5 — Data arriving in PostgreSQL in real time

**What to validate**: rows are being inserted into Postgres as Spark processes each micro-batch.

**Open psql and monitor**:

```bash
docker exec -it postgres psql -U trips -d trips
```

```sql
-- Run this and leave it running — refreshes every 2 seconds
SELECT count(*) FROM trip_clusters;
\watch 2
```

**Expected output** during processing:

```
 count
-------
     0

 count
-------
    12

 count
-------
    45

 count
-------
    72
```

Each counter update represents one **Spark micro-batch** being persisted to Postgres. When it stops growing, Job 2 has caught up and is waiting for new data.

**Inspect the inserted data**:

```sql
-- Top clusters by volume
SELECT origin_cell, destination_cell, time_bucket, trip_count, iso_week
FROM trip_clusters
ORDER BY trip_count DESC
LIMIT 10;

-- Distribution by ISO week
SELECT iso_week, SUM(trip_count) AS total_trips
FROM trip_clusters
GROUP BY iso_week
ORDER BY iso_week;
```

**Deduplication test** — upload the same CSV twice:

```bash
curl -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions
```

With `\watch 2` still running, the `count(*)` **should not increase** — Silver deduplicates by `trip_id`, so Job 2 receives no new rows to aggregate.

---

### Validation summary

```
Stage 1 — API
  curl POST /ingestions  →  status: accepted, row_count: N
  curl GET  /ingestions/<id>/events  →  SSE: processing → done

Stage 2 — Kafka
  kafka-run-class GetOffsetShell  →  offset = N (matches row_count)
  kafka-console-consumer  →  JSON events printed to terminal

Stage 3 — Job 1 (Bronze/Silver)
  ls data/delta/bronze_trips/  →  Parquet files + _delta_log
  ls data/delta/silver_trips/  →  same
  Spark UI http://localhost:8080  →  job1 ACTIVE, batch_id incrementing

Stage 4 — Job 2 (Gold)
  ls data/delta/gold_trip_clusters/  →  Parquet files + _delta_log
  Spark UI  →  job2 ACTIVE, batch_id incrementing

Stage 5 — PostgreSQL
  SELECT count(*) FROM trip_clusters; \watch 2  →  counter rising in real time
```

---

## Connect to PostgreSQL via DBeaver

### Connection settings

1. Open DBeaver → **New Database Connection** → select **PostgreSQL**
2. Fill in:

| Field | Value |
|---|---|
| **Host** | `localhost` |
| **Port** | `5432` |
| **Database** | `trips` |
| **Username** | `trips` |
| **Password** | `trips` |

3. Click **Test Connection** → should show "Connected"
4. Click **Finish**

> The `postgres` container exposes port `5432` on the host. Credentials are defined in `docker-compose.yml`.

### Browse the data in DBeaver

After connecting, navigate to: `trips → Schemas → public → Tables`

Available tables:

| Table | Description |
|---|---|
| `trip_clusters` | Gold aggregates inserted by the Spark Streaming job |

To monitor inserts in real time: open a **SQL Editor**, run `SELECT count(*) FROM trip_clusters;` repeatedly while the pipeline is running — the number will increase with each Spark micro-batch.

---

## Observability Interfaces

| Interface | URL | Description |
|---|---|---|
| **API** | http://localhost:8000 | FastAPI — CSV upload, SSE status |
| **API Docs** | http://localhost:8000/docs | Swagger UI |
| **Spark Master UI** | http://localhost:8080 | Active Spark jobs, workers, streaming queries |
| **Airflow UI** | http://localhost:8081 | DAGs, task runs, logs |
| **Kafka** | localhost:9092 | Broker (access via CLI tools) |
| **PostgreSQL** | localhost:5432 | DB `trips`, user `trips`, password `trips` |

### Viewing streaming queries in the Spark UI

Go to `http://localhost:8080` → click the active job's Application ID → **Structured Streaming** tab. Each query shows:

- **Status**: ACTIVE
- **Input rows/sec**: real-time ingestion rate
- **Processing time**: duration of each micro-batch
- **Batch ID**: micro-batch counter (keeps incrementing)

---

## Data Layout

### Delta Lake layers (under `data/delta/`)

| Layer | Path | Description |
|---|---|---|
| Bronze | `data/delta/bronze_trips` | Raw Kafka data, including the original payload |
| Silver | `data/delta/silver_trips` | Valid rows, deduplicated by `trip_id` (1-day watermark) |
| Rejected | `data/delta/rejected_trips` | Invalid rows (out-of-range coordinates, null `trip_id`) |
| Gold | `data/delta/gold_trip_clusters` | Clusters aggregated by geohash × time_bucket |
| Gold | `data/delta/gold_weekly_metrics` | Weekly metrics by region |

### PostgreSQL tables (database `trips`)

| Table | Description |
|---|---|
| `trip_clusters` | Gold clusters: `origin_cell`, `destination_cell`, `time_bucket`, `trip_count`, `iso_week` |

### Kafka event schema (`trips.raw`)

```json
{
  "ingestion_id": "uuid-v4",
  "row_number": 1,
  "trip_id": "deterministic-sha256-excluding-ingestion_id",
  "region": "Prague",
  "origin_lon": 14.4973,
  "origin_lat": 50.0755,
  "destination_lon": 14.4378,
  "destination_lat": 50.0755,
  "datetime": "2018-05-28 09:03:40",
  "datasource": "funny_car"
}
```

---

## Airflow

- **UI**: `http://localhost:8081` (user: `admin`, password: `admin`)
- **Executor**: `LocalExecutor` with metadata stored in Postgres (`airflow` database)
- **DAG**: `trips_streaming_pipeline` — starts Job 1 and Job 2 in parallel via `spark-submit`
- **Timezone**: `America/Recife` (GMT-3)
- **Trigger manually**:
  ```bash
  docker exec airflow-webserver airflow dags trigger trips_streaming_pipeline \
    --run-id "trips_streaming_$(date +%Y%m%d_%H%M)"
  ```

---

## Tests

```bash
# Install dependencies
poetry install --with spark

# Run all tests
poetry run pytest

# With coverage
poetry run pytest --cov=src --cov-report=term-missing

# Unit tests only (fast, no containers needed)
poetry run pytest tests/ -k "not integration"

# Integration tests (requires Postgres running)
poetry run pytest tests/integration/
```

---

## Development

```bash
# Formatting
poetry run black src/ tests/
poetry run isort src/ tests/

# Linting
poetry run ruff check src/ tests/

# Type checking
poetry run mypy src/de_challenge/domain/ src/de_challenge/ingestion/
```

---

## Bonus Queries

See `sql/bonus_queries.sql` for analytical queries against `trip_clusters`.

---

## Technical Notes

- Geospatial grouping uses **geohash precision 7** (cells of ~153m × 153m).
- Time buckets are **30 minutes** wide for cluster grouping.
- `trip_id` is **deterministic** (SHA-256 hash excluding `ingestion_id`) to enable cross-batch deduplication.
- The status channel uses **SSE** (Server-Sent Events) — no polling, no WebSocket.
- Geohash in Spark is implemented as a **native SQL expression** (no Python UDF), maximizing executor performance.
- Kafka runs in **KRaft mode** (no Zookeeper) using the Bitnami legacy image.
