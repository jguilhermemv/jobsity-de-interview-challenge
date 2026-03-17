#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command curl
require_command python3

CSV_PATH="${1:-sample_data/trips.csv}"
[[ -f "$CSV_PATH" ]] || die "CSV file not found: $CSV_PATH"

log "Uploading CSV: $CSV_PATH"
RESPONSE_JSON="$(
  curl -sf -F "file=@${CSV_PATH};type=text/csv" http://localhost:8000/ingestions
)"

save_ingestion_response "$RESPONSE_JSON"

log "Upload accepted."
print_ingestion_summary "$RESPONSE_JSON"
printf '%s\n' "$RESPONSE_JSON"
log "Saved latest ingestion response to $LATEST_INGESTION_FILE"
