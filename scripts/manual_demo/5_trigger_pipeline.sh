#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

DAG_ID="trips_streaming_pipeline"
RUN_ID="${1:-manual_$(date +%Y%m%d_%H%M%S)}"

ensure_container_running "kafka"
ensure_container_running "airflow-webserver"

log "Creating Kafka topics required by the streaming pipeline."
docker exec kafka kafka-topics.sh \
  --create \
  --topic trips.raw \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics.sh \
  --create \
  --topic ingestion.status \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

log "Clearing previous Airflow task state."
docker exec airflow-webserver airflow tasks clear "$DAG_ID" \
  --yes --downstream --upstream 2>/dev/null || true

log "Unpausing DAG '${DAG_ID}'."
docker exec airflow-webserver airflow dags unpause "$DAG_ID"

log "Triggering DAG '${DAG_ID}' with run id '${RUN_ID}'."
docker exec airflow-webserver airflow dags trigger "$DAG_ID" --run-id "$RUN_ID"

wait_for "Job 1 first commit" 600 \
  'test -f data/checkpoints/job1_unified/commits/0'

wait_for "Job 2 gold clusters first commit" 300 \
  'test -f data/checkpoints/gold_trip_clusters/commits/0'

wait_for "Job 2 gold weekly first commit" 180 \
  'test -f data/checkpoints/gold_weekly_metrics/commits/0'

wait_for "Job 2 postgres trips first commit" 180 \
  'test -f data/checkpoints/postgres_trips/commits/0'

wait_for "Job 2 postgres trip clusters first commit" 120 \
  'test -f data/checkpoints/postgres_trip_clusters/commits/0'

wait_for "Markers completion first commit" 60 \
  'test -f data/checkpoints/markers_completion/commits/0'

log "Pipeline is hot and ready for uploads."
