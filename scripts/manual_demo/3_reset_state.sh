#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

log "Stopping long-running Spark jobs, if they exist."
if container_running "airflow-scheduler"; then
  docker exec airflow-scheduler pkill -f "job1.py" 2>/dev/null || true
  docker exec airflow-scheduler pkill -f "job2.py" 2>/dev/null || true
else
  warn "Container 'airflow-scheduler' is not running. Skipping Spark process cleanup."
fi

log "Removing Delta tables and Spark checkpoints."
rm -rf data/delta/bronze_trips \
       data/delta/silver_trips \
       data/delta/silver_ingestion_markers \
       data/delta/rejected_trips \
       data/delta/gold_trip_clusters \
       data/delta/gold_weekly_metrics \
       data/checkpoints

log "Clearing PostgreSQL serving tables."
ensure_container_running "postgres"
docker exec -i postgres psql -U trips -d trips -c \
  "TRUNCATE TABLE trips, trip_clusters, regions, datasources RESTART IDENTITY CASCADE;"

rm -f "$LATEST_INGESTION_FILE"

log "Fresh-run state cleared."
