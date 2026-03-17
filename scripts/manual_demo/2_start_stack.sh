#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command docker
require_command curl

mkdir -p data/airflow data/delta data/checkpoints

log "Stopping previous containers and volumes."
compose down --volumes || true

log "Starting the full stack with fresh images."
compose up -d --build

wait_for "Kafka broker" 120 \
  'docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list'

wait_for "PostgreSQL process" 180 \
  'docker exec postgres pg_isready'

wait_for "PostgreSQL trips database" 120 \
  'docker exec postgres pg_isready -U trips -d trips'

wait_for "FastAPI /healthz" 90 \
  'curl -sf http://localhost:8000/healthz'

wait_for "Spark Master UI" 90 \
  'curl -sf http://localhost:8080'

log "Stack is ready."
