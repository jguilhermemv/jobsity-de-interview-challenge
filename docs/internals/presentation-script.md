# Presentation Script — Jobsity Data Engineering Challenge

## 1. Read Requirements

## 2. Architecture (Overview)

**Screen**: Excalidraw

## 3. Code — Key Design Decisions

### 3.1 Deterministic `trip_id` (Deduplication)

**File**: `src/de_challenge/domain/identifiers.py`

> "`compute_trip_id` generates a SHA-256 hash of the trip's stable fields — **excluding** `ingestion_id`. The same CSV uploaded twice produces the same `trip_id`; Spark deduplicates."

### 3.2 Geohash in Spark SQL (No Python UDF)

**File**: `src/de_challenge/spark/transforms.py`

> "Geohash implemented as native Spark expressions — no Python UDF. This design is crucial for efficient spatial grouping of trips: geohash encodes latitude and longitude into a short string, allowing us to group trips by area using fast SQL grouping rather than slower spatial operations. Implementing geohash natively in Spark (not as a Python UDF) ensures the logic is distributed, highly parallel, and scalable up to 100M records without bottlenecks."

### 3.3 Job 1 — Watermark and Deduplication

**File**: `src/de_challenge/spark/job1.py`

> "We use `withWatermark('kafka_timestamp', '1 day')` to instruct Spark to track late data and to define how long state should be kept for each unique record—in our case, up to 1 day after its event time. This limits memory usage by allowing old state to be safely discarded. Combined with `dropDuplicates(['trip_id'])`, this approach enables efficient, scalable deduplication: only events with the same `trip_id` and within the watermark window are considered duplicates, ensuring that even if events arrive out of order or are replayed, true duplicates are removed without unbounded memory growth. The watermark is essential when processing streaming data at scale, as it balances deduplication correctness against system resource usage."

---

## 4. Bring Up the Stack (Manual Operations)

### 4.4 Upload CSV

```bash
curl -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions
```

Note the returned `ingestion_id`.

### 4.5 Status via SSE (No Polling)

```bash
curl -N http://localhost:8000/ingestions/<ingestion_id>/events
```

Expected milestone sequence during the demo:

```text
STARTED → IN_PROGRESS → COMPLETED → BRONZE_COMPLETED → SILVER_COMPLETED → TRIPS_PG_COMPLETED
```

## 6. Scalability and Cloud


## 7. Closing

> "Summary: ingestion via API (202), Kafka as backbone, Spark Structured Streaming, Delta Lake, PostgreSQL/PostGIS. Status via SSE without polling. Deduplication by `trip_id`. Validation done via Airflow, Kafka in terminal, Delta folders, and DBeaver."



