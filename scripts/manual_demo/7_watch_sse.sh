#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command curl

INGESTION_ID="${1:-}"
if [[ -z "$INGESTION_ID" ]]; then
  INGESTION_ID="$(latest_ingestion_id)"
fi

log "Streaming SSE events for ingestion_id=${INGESTION_ID}"
exec curl -N "http://localhost:8000/ingestions/${INGESTION_ID}/events"
