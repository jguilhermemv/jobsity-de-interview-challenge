#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

ensure_container_running "postgres"

log "Showing the top grouped trips from trip_clusters."
docker exec -i postgres psql -U trips -d trips -c \
  "SELECT origin_cell, destination_cell, time_bucket, trip_count, iso_week FROM trip_clusters ORDER BY trip_count DESC LIMIT 10;"
