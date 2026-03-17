#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command curl

REGION_URL="http://localhost:8000/trips/weekly-average?region=Prague"
BBOX_URL="http://localhost:8000/trips/weekly-average?min_lon=14.0&min_lat=49.9&max_lon=14.6&max_lat=50.2"

log "Querying weekly average by region."
curl -sf "$REGION_URL"
printf '\n'

log "Querying weekly average by bounding box."
curl -sf "$BBOX_URL"
printf '\n'
