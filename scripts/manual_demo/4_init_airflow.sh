#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

log "Initializing the Airflow metadata schema."
compose run --rm airflow-webserver airflow db migrate

log "Creating the Airflow admin user."
compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || warn "Admin user may already exist."

log "Starting Airflow services."
compose up -d airflow-webserver airflow-scheduler

wait_for "Airflow webserver" 180 \
  'curl -sf http://localhost:8081/health | grep -q healthy'

log "Airflow is ready at http://localhost:8081 (admin/admin)."
