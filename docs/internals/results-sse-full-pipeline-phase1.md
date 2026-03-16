# Results — SSE Full Pipeline Phase 1

## What Was Implemented

Phase 1 of the [implementation plan](implementation-plan-sse-full-pipeline.md) is complete. The `GET /ingestions/{id}/events` SSE endpoint now reports ingestion progress beyond the API publish phase, through Bronze, Silver, and PostgreSQL trips — without polling and without emitting false positives.

### Implemented Components

| Component | Changes |
|----------|---------|
| **API** | Kafka key changed from `trip_id` to `ingestion_id`; publishes one `ingestion_end` marker after all trip events; marker has `record_type`, `control_id`, `total_rows` |
| **Contract** | Added `IngestionEndEvent`; `TripEvent` has optional `record_type` |
| **Job 1** | Flexible JSON parsing (trip vs marker); unified `foreachBatch` for Bronze + Silver + markers; emits `BRONZE_COMPLETED` and `SILVER_COMPLETED` after sink commits; new `silver_ingestion_markers` Delta table |
| **Status Publisher** | New `src/de_challenge/spark/status_publisher.py` for Spark-side status emission |
| **Job 2** | Added `ingestion_id` to PostgreSQL `trips`; new stream on `silver_ingestion_markers`; verifies PG count per ingestion and emits `TRIPS_PG_COMPLETED` |
| **PostgreSQL** | New `ingestion_id` column and index on `trips`; migration script for existing deployments |

### Status Sequence (Phase 1)

```
STARTED → IN_PROGRESS → COMPLETED → BRONZE_COMPLETED → SILVER_COMPLETED → TRIPS_PG_COMPLETED
```

Each status is emitted only after the corresponding sink has committed successfully.

---

## How to Verify

### 1. Run the pipeline

```bash
bash demo.sh
```

Or manually: start Docker Compose, trigger the DAG, upload a CSV, then connect to SSE:

```bash
curl -N http://localhost:8000/ingestions/<ingestion_id>/events
```

### 2. Expected SSE sequence

For a successful ingestion of N rows:

1. `STARTED`
2. `IN_PROGRESS` (every 100 rows)
3. `COMPLETED` (API finished publishing to Kafka)
4. `BRONZE_COMPLETED` (Bronze Delta append succeeded)
5. `SILVER_COMPLETED` (Silver trips + markers written)
6. `TRIPS_PG_COMPLETED` (PostgreSQL has N rows for that `ingestion_id`)

### 3. Unit tests

```bash
poetry run pytest tests/ -v -k "not spark and not postgis"
```

### 4. Create Kafka topic for status (if needed)

The `ingestion.status` topic is typically auto-created when the API first publishes. If not:

```bash
docker exec kafka kafka-topics.sh \
  --create --topic ingestion.status \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 \
  --if-not-exists
```

---

## Known Limitations

- **Phase 2 not implemented**: `GOLD_DELTA_COMPLETED`, `CLUSTERS_PG_COMPLETED`, and `PIPELINE_COMPLETED` are not emitted. Phase 2 would require per-ingestion audit for aggregate sinks and a finalizer/coordinator.
- **Checkpoint change**: Job 1 now uses a single checkpoint `job1_unified`. A full reset clears old checkpoints; reprocessing from scratch requires clearing `data/checkpoints/` and Delta tables.
- **Existing PostgreSQL**: Run `sql/migrations/001_add_ingestion_id_to_trips.sql` if upgrading from a schema without `ingestion_id`.
- **Empty ingestion**: API publishes one marker with `total_rows=0`; downstream emits completion without business rows.

---

## Rollback

If issues occur:

1. Stop publishing the marker in the API (revert `_publish_events`).
2. Disable Job 1 / Job 2 status emission (remove `publish_ingestion_status` calls).
3. Keep existing API-only SSE behavior (`STARTED`, `IN_PROGRESS`, `COMPLETED`).

New columns and tables are additive; rollback is safe.
