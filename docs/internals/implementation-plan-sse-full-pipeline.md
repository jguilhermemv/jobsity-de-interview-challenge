# Implementation Plan — SSE Status for Full Pipeline (Robust Version)

## Goal

Extend `GET /ingestions/{id}/events` so it reports ingestion progress beyond the API publish phase, without polling and without emitting false positives.

This plan intentionally separates:

- **Phase 1 (recommended now)**: reliable status for `API -> Kafka -> Bronze -> Silver -> PostgreSQL trips`
- **Phase 2 (optional, for true end-to-end completion)**: add status for Gold aggregates and emit a final `PIPELINE_COMPLETED`

This split is deliberate. With the current architecture, claiming "full pipeline completed" as soon as Job 2 writes `trips` to PostgreSQL would be inaccurate because Gold Delta and `trip_clusters` are written by separate streaming queries.

---

## What Must Be True

For this feature to be trustworthy, each emitted status must mean:

- the stage **actually committed successfully**
- not just that Spark **saw a marker**
- not just that a row **reached a micro-batch**

Therefore, completion events must be emitted from the same `foreachBatch` that writes the corresponding sink, or from a downstream audit/finalizer that observes committed sink-level completion.

---

## Current vs Target Behavior

| Stage | Current | Phase 1 Target | Phase 2 Target |
|-------|---------|----------------|----------------|
| API receives CSV | `STARTED` | `STARTED` | `STARTED` |
| API publishing to Kafka | `IN_PROGRESS`, `COMPLETED` | `IN_PROGRESS`, `COMPLETED` | `IN_PROGRESS`, `COMPLETED` |
| Job 1 raw persisted to Bronze | — | `BRONZE_COMPLETED` | `BRONZE_COMPLETED` |
| Job 1 clean rows persisted to Silver | — | `SILVER_COMPLETED` | `SILVER_COMPLETED` |
| Job 2 trips persisted to PostgreSQL | — | `TRIPS_PG_COMPLETED` | `TRIPS_PG_COMPLETED` |
| Job 2 aggregates persisted | — | — | `GOLD_DELTA_COMPLETED`, `CLUSTERS_PG_COMPLETED` |
| All required sinks done | — | — | `PIPELINE_COMPLETED` |
| Any stage failed | implicit / missing | `FAILED` | `FAILED` |

Notes:

- Keep existing `COMPLETED` semantics as **"API finished publishing to Kafka"** to minimize API changes.
- Do not overload `COMPLETED` to mean "entire pipeline finished".

---

## Key Design Decisions

### 1. Use an End-of-Ingestion Marker in Kafka

After publishing all trip events, the API publishes one extra control event to `trips.raw`:

```json
{
  "record_type": "ingestion_end",
  "ingestion_id": "uuid",
  "control_id": "uuid:end",
  "total_rows": 123
}
```

Why:

- it gives downstream consumers a deterministic boundary for one ingestion
- it avoids polling for "did more rows arrive?"

### 2. Use `ingestion_id` as Kafka Key for Both Trip Events and Marker

All events belonging to one ingestion must use `key=ingestion_id`.

Why:

- Kafka ordering is guaranteed only within a partition
- the marker must be published after all trip events for that ingestion and arrive in that same order downstream

### 3. Keep Control Flow Separate from Business Semantics

Do **not** treat the marker as a normal trip row.

Why:

- Silver currently deduplicates by `trip_id`
- markers do not have a business `trip_id`
- mixing control records into business tables increases risk of broken dedup, validation, and aggregation logic

Recommended split:

- `trips.raw` topic contains both business events and the end marker
- Bronze can store the raw marker because Bronze is the raw/audit layer
- Silver business data remains trip-only
- control markers are also written to a dedicated Delta table, e.g. `silver_ingestion_markers`

### 4. Emit Status Only After Sink Commit

Never publish `BRONZE_COMPLETED`, `SILVER_COMPLETED`, or `TRIPS_PG_COMPLETED` merely because the marker was observed in a parsed stream.

Why:

- the current jobs write to multiple independent streaming queries
- seeing a marker upstream does not prove the downstream sink has committed

### 5. Treat "Full Pipeline Completed" as a Separate Concern

Phase 2 should add an explicit finalizer/coordinator that emits `PIPELINE_COMPLETED` only after all required stage-completion events have been observed for the ingestion.

Why:

- Job 2 writes to multiple sinks independently
- there is no single place today that can truthfully say "everything is done"

---

## Event Model

### Existing Statuses

- `STARTED`
- `IN_PROGRESS`
- `COMPLETED` = API finished publishing all rows and the end marker to Kafka

### New Statuses for Phase 1

- `BRONZE_COMPLETED`
- `SILVER_COMPLETED`
- `TRIPS_PG_COMPLETED`
- `FAILED`

### Additional Statuses for Phase 2

- `GOLD_DELTA_COMPLETED`
- `CLUSTERS_PG_COMPLETED`
- `PIPELINE_COMPLETED`

### Failure Payload

When possible, use:

```json
{
  "ingestion_id": "uuid",
  "status": "FAILED",
  "stage": "silver",
  "retryable": true,
  "error": "human-readable summary"
}
```

---

## Implementation Plan

### Step 1 — API: Publish the Marker and Fix Kafka Partitioning

**File**: `src/de_challenge/api/main.py`

Changes:

- change trip event key from `trip_id` to `ingestion_id`
- after the last trip event, publish exactly one end marker with `record_type="ingestion_end"`
- call `flush()` after all trip events plus marker are sent
- keep `publish_status(..., "COMPLETED", details={"rows": row_count})` as the API-level completion signal

Recommended marker payload:

```python
{
    "record_type": "ingestion_end",
    "ingestion_id": ingestion_id,
    "control_id": f"{ingestion_id}:end",
    "total_rows": row_count,
}
```

Notes:

- `control_id` gives the marker its own stable identifier
- `trip_id` remains deterministic and unchanged for business events

### Step 2 — Contract: Make Control Events Explicit

**File**: `src/de_challenge/contract/events.py`

Add an explicit contract instead of a comment-only convention:

- `TripEvent`
- `IngestionEndEvent`

Recommended minimum fields for `IngestionEndEvent`:

- `record_type: Literal["ingestion_end"]`
- `ingestion_id: str`
- `control_id: str`
- `total_rows: int`

This avoids schema ambiguity in Spark and makes tests easier.

### Step 3 — Job 1: Split Business Rows from Control Rows

**File**: `src/de_challenge/spark/job1.py`

#### 3a. Parse a Flexible Kafka Payload

Read `value` as JSON and identify:

- trip rows: `record_type` missing or `record_type == "trip"`
- marker rows: `record_type == "ingestion_end"`

Do not rely on null-checking trip fields alone to infer control rows.

#### 3b. Bronze Path

Recommended behavior:

- Bronze stores the raw payload for all events from `trips.raw`, including the marker
- this is acceptable because Bronze is the raw/audit layer

Implementation note:

- replace the current simple Bronze `writeStream.format("delta")` with a `foreachBatch` writer
- inside the same batch function:
  - append the batch to Bronze
  - detect whether the ingestion-end marker exists in that batch
  - publish `BRONZE_COMPLETED` only after the append succeeds

Why this refactor matters:

- it ties the status event to the actual Bronze commit

#### 3c. Silver Business Path

Silver should contain **trip rows only**.

Implementation:

- filter valid trip rows using the existing validation logic
- keep `withWatermark(...).dropDuplicates(["trip_id"])` only on trip rows
- do not send markers through the business Silver table

This avoids the current deduplication problem entirely.

#### 3d. Silver Control Path

Write marker rows to a dedicated control Delta table, e.g.:

- `data/delta/silver_ingestion_markers`

This table should include at least:

- `ingestion_id`
- `control_id`
- `total_rows`
- `kafka_timestamp`

Implementation note:

- use a second `foreachBatch` or an explicit append path dedicated to markers
- publish `SILVER_COMPLETED` only after:
  - trip rows for that batch were successfully written to Silver
  - marker rows for that batch were successfully written to `silver_ingestion_markers`

### Step 4 — Shared Spark Status Publisher

**New file**: `src/de_challenge/spark/status_publisher.py`

Create one shared helper for Spark-side status emission:

- `publish_ingestion_status(ingestion_id: str, status: str, details: dict | None = None)`

Requirements:

- uses `KAFKA_BOOTSTRAP_SERVERS` and `STATUS_TOPIC`
- creates a producer lazily or caches one safely in the driver
- publishes JSON payloads compatible with the API SSE consumer

This avoids duplicating Kafka producer setup across Job 1 and Job 2.

### Step 5 — Job 2 Phase 1: Reliable PostgreSQL Trip Completion

**File**: `src/de_challenge/spark/job2.py`

Phase 1 should focus on the `trips` table only.

#### 5a. Keep Business Stream on `silver_trips`

The existing business stream can keep reading trip rows from Silver and writing them to PostgreSQL.

#### 5b. Add Control Stream on `silver_ingestion_markers`

Create a second stream that reads the marker table and decides when `TRIPS_PG_COMPLETED` is safe to emit.

Recommended prerequisite:

- add `ingestion_id` to the PostgreSQL `trips` table so completion can be verified per ingestion

Why:

- without `ingestion_id` in the sink, Job 2 cannot reliably verify that all rows for a specific ingestion reached PostgreSQL

#### 5c. Completion Check for PostgreSQL Trips

For each marker:

- read `ingestion_id` and `total_rows`
- query PostgreSQL for `COUNT(*)` of rows with that `ingestion_id`
- retry briefly until the count reaches `total_rows`
- emit `TRIPS_PG_COMPLETED` only then

This is more reliable than assuming "marker seen" means "all trips are already in PostgreSQL".

### Step 6 — Phase 2: True End-to-End Completion for Gold and Aggregates

Phase 2 is required if the product requirement is truly:

> "tell the user when the whole pipeline, including Gold/aggregates, has finished"

With the current architecture, this needs explicit sink-level auditing.

#### 6a. Add Per-Ingestion Audit for Aggregate Sinks

Each aggregate sink should emit or persist one completion record per ingestion:

- Gold Delta refresh completed for that ingestion
- `trip_clusters` PostgreSQL refresh completed for that ingestion

Possible implementations:

- publish stage-completion events directly to `ingestion.status`
- or write audit rows to a dedicated Delta/Postgres audit table and then publish from a finalizer

#### 6b. Add a Finalizer / Coordinator

A lightweight coordinator should consume stage-completion events and emit `PIPELINE_COMPLETED` only after all required statuses are present for the same `ingestion_id`.

Required statuses for Phase 2:

- `COMPLETED`
- `BRONZE_COMPLETED`
- `SILVER_COMPLETED`
- `TRIPS_PG_COMPLETED`
- `GOLD_DELTA_COMPLETED`
- `CLUSTERS_PG_COMPLETED`

This finalizer can live:

- in the API process as a background worker, or
- as a small dedicated consumer process

### Step 7 — API SSE Consumer and Documentation

**File**: `src/de_challenge/api/main.py`

Transport behavior can remain the same, but the exposed contract must change.

Required updates:

- document the new statuses in the SSE endpoint description
- update any examples that currently mention only `STARTED`, `IN_PROGRESS`, `COMPLETED`

No new endpoint is needed.

### Step 8 — Tests (TDD)

Follow the repository workflow: tests first, then implementation.

#### Unit Tests

- API publishes trip events with `key=ingestion_id`
- API publishes exactly one end marker after all trip events
- Job 1 correctly distinguishes trip rows from marker rows
- Job 1 deduplicates only trip rows, never control rows
- Job 1 publishes `BRONZE_COMPLETED` only after Bronze write succeeds
- Job 1 publishes `SILVER_COMPLETED` only after Silver and marker-control writes succeed
- Job 2 emits `TRIPS_PG_COMPLETED` only after PostgreSQL count reaches `total_rows`

#### Contract Tests

- `TripEvent` and `IngestionEndEvent` serialize as expected
- Spark parsing accepts both event shapes

#### Integration Tests

- one ingestion produces statuses in the expected order through Phase 1
- replay / restart does not corrupt sink state
- duplicate completion events are either prevented or tolerated idempotently

#### Smoke Tests

- micro-batch run with a tiny CSV in Docker Compose
- SSE client receives all expected events without polling

### Step 9 — Results File

After implementation, add a validation artifact such as `results.md` with:

- what was implemented
- how to run the verification
- observed SSE sequence
- known limitations, especially if only Phase 1 was completed

---

## Expected Event Sequences

### Phase 1

```text
STARTED
IN_PROGRESS
COMPLETED
BRONZE_COMPLETED
SILVER_COMPLETED
TRIPS_PG_COMPLETED
```

### Phase 2

```text
STARTED
IN_PROGRESS
COMPLETED
BRONZE_COMPLETED
SILVER_COMPLETED
TRIPS_PG_COMPLETED
GOLD_DELTA_COMPLETED
CLUSTERS_PG_COMPLETED
PIPELINE_COMPLETED
```

---

## Edge Cases

| Case | Handling |
|------|----------|
| Multiple ingestions interleaved | Use `key=ingestion_id` so trip rows and marker preserve order per ingestion. |
| Empty ingestion (`0` rows) | API still publishes one marker with `total_rows=0`; downstream should emit completion without business rows. |
| Job restart after marker processed | Completion events must be idempotent or deduplicated by `(ingestion_id, status)`. |
| Partial PostgreSQL write then retry | Use existing idempotent insert strategy plus completion verification by `COUNT(*)`. |
| Marker reaches Spark before sink catches up | Do not emit completion from marker detection alone; only emit after sink-level verification. |

---

## Dependency / Schema Impact

Required or strongly recommended:

- `kafka-python` available in Spark driver environment
- PostgreSQL `trips` table stores `ingestion_id` for per-ingestion completion checks
- new Delta control table: `silver_ingestion_markers`

Optional for Phase 2:

- audit table or finalizer process for aggregate sink completion

---

## Rollback

If the change causes issues:

1. stop publishing the marker in the API
2. disable Job 1 / Job 2 status emission for the new stages
3. keep the existing API-only SSE behavior (`STARTED`, `IN_PROGRESS`, `COMPLETED`)

This rollback is safe as long as new sink columns/tables are additive.

---

## Documentation Updates

After implementation, update:

- `README.md` to explain the new SSE semantics
- `docs/architecture-en-us.md` to reflect marker + control flow
- `docs/solution-architecture-proposal-en-us.md` if the chosen scope becomes "Phase 1 first, Phase 2 optional"
- `docs/internals/presentation-script.md` to match the actual status sequence demonstrated
