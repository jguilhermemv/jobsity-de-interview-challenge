#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STATE_DIR="$REPO_ROOT/.demo_state"
LATEST_INGESTION_FILE="$STATE_DIR/latest_ingestion.json"

cd "$REPO_ROOT"

log() {
  printf '[manual-demo] %s\n' "$*"
}

warn() {
  printf '[manual-demo] WARN: %s\n' "$*" >&2
}

die() {
  printf '[manual-demo] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

detect_compose() {
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE=(docker-compose)
  else
    die "docker compose not found. Install Docker Compose V2 or docker-compose."
  fi
}

compose() {
  detect_compose
  "${DOCKER_COMPOSE[@]}" "$@"
}

wait_for() {
  local label="$1"
  local timeout_seconds="$2"
  local expression="$3"
  local waited=0

  log "Waiting for ${label}..."
  while ! eval "$expression" >/dev/null 2>&1; do
    if (( waited >= timeout_seconds )); then
      die "Timeout waiting for ${label} after ${timeout_seconds}s."
    fi
    sleep 2
    waited=$((waited + 2))
  done

  log "${label} is ready."
}

container_running() {
  local container_name="$1"
  local state

  state="$(docker inspect -f '{{.State.Running}}' "$container_name" 2>/dev/null || true)"
  [[ "$state" == "true" ]]
}

ensure_container_running() {
  local container_name="$1"

  container_running "$container_name" || die "Container '${container_name}' is not running."
}

ensure_state_dir() {
  mkdir -p "$STATE_DIR"
}

save_ingestion_response() {
  local response_json="$1"

  ensure_state_dir
  printf '%s\n' "$response_json" > "$LATEST_INGESTION_FILE"
}

latest_ingestion_id() {
  [[ -f "$LATEST_INGESTION_FILE" ]] || die "No saved ingestion found. Run 6_upload_csv.sh first or pass the ingestion_id explicitly."

  python3 - "$LATEST_INGESTION_FILE" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)

print(data["ingestion_id"])
PY
}

print_ingestion_summary() {
  local response_json="$1"

  RESPONSE_JSON="$response_json" python3 <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE_JSON"])
print(f"ingestion_id={data['ingestion_id']}")
print(f"rows={data['rows']}")
PY
}
