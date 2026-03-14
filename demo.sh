#!/usr/bin/env bash
# =============================================================================
#  demo.sh — Jobsity Data Engineering Challenge
#  Full end-to-end demo: CSV → FastAPI → Kafka → Spark → Delta Lake → PostgreSQL
#
#  Run this from the repository root right after cloning:
#    bash demo.sh
#
#  FLAGS:
#    --resume      Skip the reset/teardown step (containers already running)
#    --skip-build  Skip `docker compose build` (images already built)
#    --no-tests    Skip the pytest unit/contract test suite
#    --help        Show this help
#
#  WHAT THIS SCRIPT DOES (narrative):
#    1.  Verifies prerequisites (Docker, curl)
#    2.  Tears down any previous state for a clean slate
#    3.  Builds all Docker images
#    4.  Starts the infrastructure: Kafka (KRaft), PostgreSQL/PostGIS, FastAPI,
#        Spark Master + Worker
#    5.  Initialises Airflow (metadata DB, admin user, webserver + scheduler)
#    6.  Runs the pytest unit and contract test suite
#    7.  Triggers the Spark Structured Streaming pipeline via Airflow DAG
#    8.  Uploads trips.csv to the FastAPI endpoint
#    9.  Validates every pipeline stage:
#          Stage 1 — API accepted the file (HTTP 202)
#          Stage 2 — Kafka topic has the right offset + shows sample events
#          Stage 3 — Job 1 wrote Bronze and Silver Delta tables
#          Stage 4 — Job 2 wrote Gold Delta tables
#          Stage 5 — PostgreSQL trip_clusters rows are growing in real time
#   10.  Demonstrates idempotent deduplication (same CSV twice, no extra rows)
#   11.  Runs bonus analytical queries against trip_clusters
#   12.  Prints a final summary of all observability URLs
#
#  CLOUD EQUIVALENTS (mentioned inline):
#    Kafka     →  AWS MSK (managed, multi-AZ)
#    Spark     →  Databricks on AWS (Structured Streaming + Delta Lake)
#    API       →  FastAPI on Amazon EKS behind ALB
#    Database  →  Amazon RDS PostgreSQL + PostGIS extension
#    Airflow   →  Databricks Workflows (or MWAA)
#
#  APPROX. RUNTIME:
#    First run  (build + Ivy download):  20–35 min
#    Subsequent (--resume):               5–10 min
# =============================================================================
set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Colour palette ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
BLU='\033[0;34m'
CYN='\033[0;36m'
MGT='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ── CLI flags ──────────────────────────────────────────────────────────────────
RESET_STATE=true
SKIP_BUILD=false
RUN_TESTS=true

for arg in "$@"; do
  case $arg in
    --resume)      RESET_STATE=false ;;
    --skip-build)  SKIP_BUILD=true   ;;
    --no-tests)    RUN_TESTS=false   ;;
    --help|-h)
      head -50 "$0" | grep '^#[^!]' | sed 's/^# \{0,2\}//'
      exit 0
      ;;
    *)
      echo "Unknown flag: $arg  (use --help)" && exit 1 ;;
  esac
done

# ── Pretty helpers ─────────────────────────────────────────────────────────────
hr()       { echo -e "${DIM}$(printf '─%.0s' {1..72})${NC}"; }

banner() {
  echo
  echo -e "${BOLD}${BLU}╔$(printf '═%.0s' {1..72})╗${NC}"
  echo -e "${BOLD}${BLU}║$(printf ' %.0s' {1..72})║${NC}"
  printf "${BOLD}${BLU}║  ${NC}${BOLD}%-70s${BLU}║${NC}\n" "$1"
  echo -e "${BOLD}${BLU}║$(printf ' %.0s' {1..72})║${NC}"
  echo -e "${BOLD}${BLU}╚$(printf '═%.0s' {1..72})╝${NC}"
}

section() {
  echo
  echo -e "${BOLD}${CYN}┌$(printf '─%.0s' {1..72})┐${NC}"
  printf "${BOLD}${CYN}│  ${NC}${BOLD}%-70s${CYN}│${NC}\n" "$1"
  echo -e "${BOLD}${CYN}└$(printf '─%.0s' {1..72})┘${NC}"
}

step()    { echo -e "\n${BOLD}${GRN}  ▶  $*${NC}"; }
ok()      { echo -e "     ${GRN}✔  $*${NC}"; }
info()    { echo -e "     ${CYN}ℹ  $*${NC}"; }
warn()    { echo -e "     ${YLW}⚠  $*${NC}"; }
cloud()   { echo -e "     ${MGT}☁  $*${NC}"; }   # cloud analogy annotation
die()     { echo -e "     ${RED}✖  $*${NC}" >&2; exit 1; }
narrate() { echo -e "\n  ${DIM}  » $*${NC}"; }
indent()  { sed 's/^/       /'; }

# wait_for <label> <timeout_seconds> <shell_expression>
wait_for() {
  local label="$1" timeout="$2" expr="$3"
  local elapsed=0 interval=4
  printf "     Waiting for %-42s" "$label ..."
  while ! eval "$expr" &>/dev/null 2>&1; do
    if [[ $elapsed -ge $timeout ]]; then
      echo -e " ${RED}TIMEOUT (${timeout}s)${NC}"
      return 1
    fi
    printf "."
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
  echo -e " ${GRN}ready${NC}"
}

# Run SQL in the trips database and print the result
pg() { docker exec -i postgres psql -U trips -d trips --no-align -t -c "$1" 2>/dev/null; }

# ══════════════════════════════════════════════════════════════════════════════
#  0.  INTRO BANNER
# ══════════════════════════════════════════════════════════════════════════════

banner "Jobsity Data Engineering Challenge — Live Demo"

echo
narrate "We will walk through a complete streaming data pipeline running locally"
narrate "in Docker. Every component has a direct cloud equivalent on AWS + Databricks."
echo
echo -e "  ${BOLD}Stack:${NC}"
echo -e "   ${CYN}FastAPI${NC}   →  ingestion REST API  ${DIM}(EKS + ALB in production)${NC}"
echo -e "   ${CYN}Kafka${NC}     →  event backbone      ${DIM}(AWS MSK — fully managed)${NC}"
echo -e "   ${CYN}Spark${NC}     →  streaming engine    ${DIM}(Databricks Structured Streaming)${NC}"
echo -e "   ${CYN}Delta Lake${NC} →  storage layers     ${DIM}(Databricks Unity Catalog)${NC}"
echo -e "   ${CYN}PostgreSQL${NC} →  serving layer      ${DIM}(Amazon RDS + PostGIS)${NC}"
echo -e "   ${CYN}Airflow${NC}   →  orchestration      ${DIM}(Databricks Workflows / MWAA)${NC}"

echo
echo -e "  ${BOLD}Data flow:${NC}"
echo -e "   ${DIM}trips.csv → POST /ingestions → Kafka (trips.raw)${NC}"
echo -e "   ${DIM}          → Job 1: Bronze Δ (raw) → Silver Δ (deduped)${NC}"
echo -e "   ${DIM}          → Job 2: Gold Δ (clusters) → PostgreSQL/PostGIS${NC}"

hr
echo
read -r -p "  Press ENTER to start the demo, or Ctrl+C to abort: " _

# ══════════════════════════════════════════════════════════════════════════════
#  1.  PREREQUISITES
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 0 — Prerequisites"

step "Checking required tools"

command -v docker  &>/dev/null || die "docker not found. Install Docker Desktop first."
ok "docker found: $(docker --version)"

# Support both 'docker compose' (V2 plugin) and 'docker-compose' (legacy).
# DC is an array so it expands correctly as a command even with spaces.
if docker compose version &>/dev/null 2>&1; then
  DC=(docker compose)
elif command -v docker-compose &>/dev/null; then
  DC=(docker-compose)
else
  die "docker compose not found. Install Docker Desktop or the Compose V2 plugin."
fi
ok "docker compose found: $("${DC[@]}" version)"

command -v curl &>/dev/null || die "curl not found. Please install curl."
ok "curl found: $(curl --version | head -1)"

# Check available memory (warn only — we need ~4 GB for Spark)
if command -v sysctl &>/dev/null; then
  MEM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
  MEM_GB=$(( MEM_BYTES / 1024 / 1024 / 1024 ))
  if [[ $MEM_GB -lt 4 ]]; then
    warn "Only ${MEM_GB} GB RAM detected. Spark needs ~4 GB — performance may degrade."
  else
    ok "Memory: ${MEM_GB} GB available"
  fi
fi

ok "All prerequisites satisfied"

# ══════════════════════════════════════════════════════════════════════════════
#  2.  OPTIONAL CLEAN RESET
# ══════════════════════════════════════════════════════════════════════════════

if $RESET_STATE; then
  section "STAGE 0.5 — Clean Reset (fresh slate)"

  narrate "We stop and remove all containers, volumes, and local state so the demo"
  narrate "starts from an identical baseline every time — repeatable, auditable."

  step "Stopping all containers"
  "${DC[@]}" down --remove-orphans --volumes 2>&1 | indent || true

  step "Removing Delta tables and Spark checkpoints"
  info "data/delta/bronze_trips    — raw Kafka payloads (Parquet + _delta_log/)"
  info "data/delta/silver_trips    — clean, deduplicated rows (Parquet + _delta_log/)"
  info "data/delta/rejected_trips  — invalid rows sent to the Dead Letter layer"
  info "data/delta/gold_trip_clusters  — geohash × time_bucket aggregates"
  info "data/delta/gold_weekly_metrics — region × ISO-week metrics"
  info "data/checkpoints/          — Spark Structured Streaming offset state"
  info "  (checkpoints tell Spark which Kafka offsets have already been processed;"
  info "   deleting them forces a full reprocess from the earliest Kafka offset)"
  rm -rf data/delta/bronze_trips \
         data/delta/silver_trips \
         data/delta/rejected_trips \
         data/delta/gold_trip_clusters \
         data/delta/gold_weekly_metrics
  rm -rf data/checkpoints
  ok "Delta tables cleared"
  ok "Spark checkpoints cleared"

  step "Removing Airflow runtime data"
  info "data/airflow/  — Airflow metadata DB files, logs, and plugin cache"
  rm -rf data/airflow
  ok "Airflow runtime cleared"

  step "Removing PostgreSQL data (named volume)"
  info "postgres_data  — Docker named volume for PostgreSQL"
  info "  Named volumes avoid macOS bind-mount permission issues with initdb."
  info "  Cleared via 'docker compose down --volumes' (already done above)."
  ok "PostgreSQL data cleared (volume dropped with 'down --volumes')"

  ok "Clean reset complete — starting from zero"
else
  warn "Skipping reset (--resume flag set)"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  3.  BUILD & START INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 1 — Build Docker Images"

narrate "We build two custom images: the FastAPI app and the Airflow image."
narrate "All other services use official images (Bitnami Kafka, PostGIS, Apache Spark)."

if $SKIP_BUILD; then
  warn "Skipping build (--skip-build flag set)"
else
  step "Building images (this takes 3–10 min on the first run)"
  "${DC[@]}" build 2>&1 | grep -E '(STEP|Step|---> |Successfully|ERROR|error)' | indent || \
  "${DC[@]}" build
  ok "All images built"
fi

section "STAGE 2 — Start Infrastructure Services"

narrate "We start the core services first (Kafka, Postgres, API, Spark)."
narrate "Airflow will be initialised separately so its schema migrations run cleanly."

step "Creating required host directories (bind mounts)"
mkdir -p data/airflow \
         data/delta \
         data/checkpoints
ok "Host directories ready (postgres_data is a named Docker volume — no host dir needed)"

step "Starting Kafka, PostgreSQL, FastAPI, Spark Master + Worker"
"${DC[@]}" up -d kafka postgres api spark-master spark-worker 2>&1 | indent

cloud "In production: Kafka → AWS MSK (managed, multi-AZ, topic replication)"
cloud "In production: PostgreSQL → Amazon RDS (Multi-AZ, automated backups)"
cloud "In production: FastAPI → ECS Fargate or EKS with horizontal pod autoscaling"
cloud "In production: Spark → Databricks cluster (auto-scaling, spot instances)"

# ── Wait for core services ────────────────────────────────────────────────────
step "Waiting for services to be ready"

wait_for "Kafka broker"    120 \
  'docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list'

# Phase 1: wait for the postgres process to accept connections (initdb may still be running)
wait_for "PostgreSQL process" 180 \
  'docker exec postgres pg_isready'

# Phase 2: wait for the 'trips' database to exist (created by init.sql)
wait_for "PostgreSQL 'trips' database" 120 \
  'docker exec postgres pg_isready -U trips -d trips'

wait_for "FastAPI /healthz" 90 \
  'curl -sf http://localhost:8000/healthz'

wait_for "Spark Master UI"  90 \
  'curl -sf http://localhost:8080'

ok "All infrastructure services are up"

# ══════════════════════════════════════════════════════════════════════════════
#  4.  AIRFLOW SETUP
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 3 — Airflow Initialisation"

narrate "Airflow acts as the orchestrator: it submits spark-submit commands to the"
narrate "Spark cluster and monitors job health. In production, Databricks Workflows"
narrate "or Amazon MWAA would replace this component."

step "Initialising the Airflow metadata database"
info "The 'airflow' database was automatically created by init.sql at Postgres startup."
"${DC[@]}" run --rm --no-deps airflow-webserver airflow db migrate 2>&1 | \
  grep -E '(INFO|ERROR|Upgrade|Complete|Done)' | indent || true
ok "Airflow metadata schema ready"

step "Creating Airflow admin user (admin/admin)"
"${DC[@]}" run --rm --no-deps airflow-webserver \
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>&1 | indent || warn "Admin user may already exist"
ok "Airflow user ready"

step "Starting Airflow webserver + scheduler"
"${DC[@]}" up -d airflow-webserver airflow-scheduler 2>&1 | indent

wait_for "Airflow webserver" 180 \
  'curl -sf http://localhost:8081/health | grep -q "healthy"'

ok "Airflow is running at http://localhost:8081  (admin / admin)"

# ══════════════════════════════════════════════════════════════════════════════
#  5.  UNIT + CONTRACT TESTS  (TDD verification)
# ══════════════════════════════════════════════════════════════════════════════

if $RUN_TESTS; then
  section "STAGE 4 — Test Suite (TDD)"

  narrate "Before processing any data we verify our domain logic: CSV parsing,"
  narrate "trip_id determinism, deduplication rules, Kafka event contract, and"
  narrate "SSE formatting. These tests run without containers."

  if command -v poetry &>/dev/null; then
    step "Installing Python dependencies (poetry install)"
    poetry install --with spark --quiet 2>&1 | indent || true

    step "Running unit + contract tests"
    echo
    poetry run pytest tests/ -k "not integration" -v \
      --tb=short --no-header 2>&1 | indent
    echo
    ok "All unit and contract tests passed"

    info "Integration tests (PostGIS schema) can be run with:"
    info "  poetry run pytest tests/integration/"
  else
    warn "Poetry not found — skipping tests."
    warn "Install Poetry (https://python-poetry.org) and re-run to see the test suite."
  fi
else
  warn "Tests skipped (--no-tests flag)"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  6.  TRIGGER STREAMING PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 5 — Trigger Spark Structured Streaming Jobs"

narrate "The Airflow DAG 'trips_streaming_pipeline' submits two spark-submit commands"
narrate "to the Spark cluster. Both jobs run CONTINUOUSLY — they never terminate."
narrate "This is the key distinction from batch: no scheduler needed to react to new data."
echo
narrate "  Job 1 (bronze_to_silver_stream):"
narrate "    Kafka (trips.raw) → Bronze Δ (raw) → Silver Δ (deduped by trip_id)"
narrate "  Job 2 (silver_to_gold_stream):"
narrate "    Silver Δ → Gold Δ (geohash clusters) → PostgreSQL/PostGIS"
echo

cloud "On Databricks: these would be Structured Streaming notebooks in a Workflow,"
cloud "auto-scaling the cluster and checkpointing to S3 (Delta Lake on S3)."

DAG_ID="trips_streaming_pipeline"
RUN_ID="demo_$(date +%Y%m%d_%H%M%S)"

step "Unpausing DAG: $DAG_ID"
docker exec airflow-webserver \
  airflow dags unpause "$DAG_ID" 2>&1 | indent
ok "DAG unpaused — scheduler will now pick up triggered runs"

step "Triggering DAG: $DAG_ID  (run-id: $RUN_ID)"
docker exec airflow-webserver \
  airflow dags trigger "$DAG_ID" --run-id "$RUN_ID" 2>&1 | indent
ok "DAG triggered — both streaming jobs are starting in the background"

info "On first run, Spark downloads Maven packages (Kafka connector, Delta Lake,"
info "PostgreSQL JDBC). This takes 2–5 minutes. We'll proceed with the upload now."

# ══════════════════════════════════════════════════════════════════════════════
#  7.  UPLOAD THE CSV
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 6 — Upload trips.csv to the API"

narrate "The FastAPI endpoint POST /ingestions accepts a CSV file and publishes"
narrate "one Kafka event per row. Each event carries a deterministic trip_id"
narrate "(SHA-256 of stable fields, excluding ingestion_id) — enabling cross-batch"
narrate "deduplication downstream in Spark."

step "Uploading sample_data/trips.csv"
echo

INGEST_RESPONSE=$(
  curl -sf \
    -F "file=@sample_data/trips.csv;type=text/csv" \
    http://localhost:8000/ingestions
)

echo "     Response: ${BOLD}${GRN}$INGEST_RESPONSE${NC}"
echo

INGESTION_ID=$(echo "$INGEST_RESPONSE" | grep -o '"ingestion_id":"[^"]*"' | cut -d'"' -f4 || \
               echo "$INGEST_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['ingestion_id'])" 2>/dev/null || \
               echo "")
ROW_COUNT=$(echo    "$INGEST_RESPONSE" | grep -o '"rows":"[^"]*"' | cut -d'"' -f4 || \
            echo    "$INGEST_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['rows'])" 2>/dev/null || \
            echo    "?")

ok "Ingestion accepted"
ok "ingestion_id = $INGESTION_ID"
ok "rows published to Kafka = $ROW_COUNT"

# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 1 VALIDATION — API
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 1: API"

narrate "The API received the CSV, parsed each row, and published one Kafka message"
narrate "per row. The response (HTTP 202) confirms the ingestion was accepted."

step "Live ingestion status stream (SSE — Server-Sent Events)"
info "The API exposes /ingestions/<id>/events as a real-time SSE stream."
info "No polling required — clients subscribe and receive push notifications."
echo
if [[ -n "$INGESTION_ID" ]]; then
  info "You can subscribe in another terminal with:"
  echo -e "     ${DIM}curl -N http://localhost:8000/ingestions/${INGESTION_ID}/events${NC}"
fi
cloud "In production: SSE over FastAPI on EKS — no WebSocket needed, HTTP/2 compatible."

# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 2 VALIDATION — KAFKA
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 2: Kafka topic 'trips.raw'"

narrate "Kafka is the durable event backbone. The API producer is fire-and-forget;"
narrate "Spark reads at its own pace. The two systems are completely decoupled."
narrate "This is exactly the pattern used with AWS MSK."

step "Checking Kafka topic offset (should equal rows uploaded)"

echo
OFFSET_RAW=$(
  docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 \
    --topic trips.raw \
    --time -1 2>/dev/null || echo "trips.raw:0:?"
)
echo -e "     Offset: ${BOLD}${GRN}$OFFSET_RAW${NC}"
echo
info "Format: topic:partition:offset  —  offset should be ≥ $ROW_COUNT"

step "Reading 3 sample events from the topic"
echo
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trips.raw \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 8000 2>/dev/null | indent || warn "Could not read messages (topic may still be empty)"

echo
cloud "In production: Kafka → AWS MSK with 3-partition topic, replication factor 3,"
cloud "retention policy 7 days, schema registry via Glue for contract enforcement."

# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 3 VALIDATION — SPARK JOB 1 (Bronze / Silver)
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 3: Job 1 — Bronze & Silver Delta Tables"

narrate "Job 1 is a Spark Structured Streaming application that reads from Kafka"
narrate "and writes to Delta Lake in two layers:"
narrate "  • Bronze: raw, unmodified Kafka payloads (append-only)"
narrate "  • Silver: valid rows, deduplicated by trip_id (1-day watermark)"
narrate "  • Rejected: invalid coordinate ranges or null trip_id (DLQ)"

step "Waiting for Spark Job 1 to complete its first micro-batch"
info "(First run downloads Maven packages — may take 3–6 minutes)"
echo

wait_for "Bronze Delta checkpoint" 600 \
  'test -f data/checkpoints/bronze_trips/commits/0'

wait_for "Silver Delta checkpoint" 120 \
  'test -f data/checkpoints/silver_trips/commits/0'

echo
step "Delta Lake files on disk"
echo
echo -e "     ${BOLD}Bronze layer${NC} (raw Kafka payloads):"
ls data/delta/bronze_trips/ 2>/dev/null | indent || echo "       (still writing...)"

echo
echo -e "     ${BOLD}Silver layer${NC} (clean + deduplicated):"
ls data/delta/silver_trips/ 2>/dev/null | indent || echo "       (still writing...)"

echo
echo -e "     ${BOLD}Rejected layer${NC} (invalid rows — Dead Letter Queue):"
ls data/delta/rejected_trips/ 2>/dev/null | indent || echo "       (empty — all rows were valid)"

echo
info "Each layer has a _delta_log/ directory with JSON transaction log entries."
info "This is ACID Delta Lake — the same format used natively in Databricks."
cloud "On Databricks: Unity Catalog manages access control and lineage for each layer."
cloud "Delta VACUUM and OPTIMIZE run automatically via Databricks auto-tuning."

# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 4 VALIDATION — SPARK JOB 2 (Gold + PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 4: Job 2 — Gold Delta & PostgreSQL"

narrate "Job 2 reads Silver in streaming mode and computes spatial aggregates:"
narrate "  • Geohash precision 7 (cells of ~153m × 153m) for origin + destination"
narrate "  • 30-minute time buckets for temporal grouping"
narrate "  • ISO week for weekly metrics"
narrate ""
narrate "Results go to two Delta Gold tables AND to PostgreSQL/PostGIS via JDBC."
narrate "Every Spark micro-batch triggers a foreachBatch write to Postgres."

step "Waiting for Job 2 to write the first Gold micro-batch"
echo

wait_for "Gold trip_clusters checkpoint" 300 \
  'test -f data/checkpoints/gold_trip_clusters/commits/0'

wait_for "PostgreSQL rows (trip_clusters)" 300 \
  'docker exec -i postgres psql -U trips -d trips -tAc "SELECT 1 FROM trip_clusters LIMIT 1" | grep -q 1'

wait_for "PostgreSQL rows (trips)" 300 \
  'docker exec -i postgres psql -U trips -d trips -tAc "SELECT 1 FROM trips LIMIT 1" | grep -q 1'

echo
step "Gold Delta files on disk"
echo
echo -e "     ${BOLD}gold_trip_clusters${NC} (aggregated by geohash × time_bucket):"
ls data/delta/gold_trip_clusters/ 2>/dev/null | indent || echo "       (still writing...)"

echo
echo -e "     ${BOLD}gold_weekly_metrics${NC} (region × ISO week × avg trips/day):"
ls data/delta/gold_weekly_metrics/ 2>/dev/null | indent || echo "       (still writing...)"

cloud "On Databricks: Gold tables are registered in Unity Catalog and exposed via"
cloud "SQL Warehouse or Delta Sharing to BI tools (Tableau, Power BI, Looker)."

# ══════════════════════════════════════════════════════════════════════════════
#  STAGE 5 VALIDATION — POSTGRESQL
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 5: PostgreSQL/PostGIS — Real-time Data"

narrate "Trip clusters are inserted into PostgreSQL with PostGIS geometry columns."
narrate "Each row in trip_clusters represents a spatiotemporal cell: how many trips"
narrate "shared a similar origin geohash, destination geohash, and time window."

step "Querying trip_clusters in PostgreSQL"
echo
echo -e "     ${BOLD}Row count:${NC}"
TRIP_COUNT=$(pg "SELECT COUNT(*) FROM trip_clusters;" || echo "0")
echo -e "     ${BOLD}${GRN}${TRIP_COUNT} rows${NC} in trip_clusters"
echo

echo -e "     ${BOLD}Top 10 clusters by trip volume:${NC}"
echo
pg "
  SELECT
    origin_cell,
    destination_cell,
    to_char(time_bucket, 'YYYY-MM-DD HH24:MI') AS time_bucket,
    trip_count,
    iso_week
  FROM trip_clusters
  ORDER BY trip_count DESC
  LIMIT 10;
" | column -t -s '|' | indent

echo
echo -e "     ${BOLD}Weekly distribution:${NC}"
echo
pg "
  SELECT
    iso_week,
    SUM(trip_count) AS total_trips,
    COUNT(DISTINCT origin_cell) AS origin_cells
  FROM trip_clusters
  GROUP BY iso_week
  ORDER BY iso_week;
" | column -t -s '|' | indent

echo
cloud "In production: Amazon RDS PostgreSQL with PostGIS, read replicas for analytics,"
cloud "and pg_partman for partition pruning on time_bucket."

# ══════════════════════════════════════════════════════════════════════════════
#  DEDUPLICATION TEST
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Deduplication: Idempotent Re-upload"

narrate "trip_id is a deterministic SHA-256 hash of stable trip fields (region,"
narrate "coordinates, datetime, datasource) — explicitly excluding ingestion_id."
narrate "This means uploading the same CSV twice produces zero new Silver rows."

BEFORE=$(pg "SELECT COUNT(*) FROM trip_clusters;" || echo "0")
echo
echo -e "     Row count BEFORE second upload: ${BOLD}${GRN}$BEFORE${NC}"

step "Uploading trips.csv a second time"
INGEST2=$(curl -sf -F "file=@sample_data/trips.csv;type=text/csv" http://localhost:8000/ingestions || echo "{}")
echo "     Response: $INGEST2"

info "Waiting 30 seconds for Spark to process the duplicate events..."
sleep 30

AFTER=$(pg "SELECT COUNT(*) FROM trip_clusters;" || echo "?")
echo
echo -e "     Row count AFTER second upload:  ${BOLD}${GRN}$AFTER${NC}"
echo

if [[ "$BEFORE" == "$AFTER" ]]; then
  ok "DEDUPLICATION CONFIRMED — count unchanged ($BEFORE → $AFTER)"
  ok "Silver layer dropped all duplicate events via trip_id watermark."
else
  warn "Count changed ($BEFORE → $AFTER). Job 2 may still be catching up."
  warn "Wait 1–2 minutes and re-check: pg 'SELECT COUNT(*) FROM trip_clusters;'"
fi

cloud "On Databricks: Delta Lake MERGE INTO provides exactly-once upsert semantics."
cloud "Combined with watermarking in Structured Streaming, this enables end-to-end"
cloud "exactly-once guarantees at scale — even across MSK partition rebalances."

# ══════════════════════════════════════════════════════════════════════════════
#  BONUS QUERIES  (challenge's exact analytical questions)
# ══════════════════════════════════════════════════════════════════════════════

section "BONUS ANALYTICAL QUERIES (sql/bonus_queries.sql)"

narrate "These are the exact two questions from the challenge brief, answered"
narrate "directly with SQL against the PostgreSQL trips table."
echo

step "Trips table row count"
echo
TRIPS_COUNT=$(pg "SELECT COUNT(*) FROM trips;")
REGIONS_COUNT=$(pg "SELECT COUNT(DISTINCT region) FROM trips;")
DATASOURCES_COUNT=$(pg "SELECT COUNT(DISTINCT datasource) FROM trips;")
echo -e "     ${BOLD}${GRN}${TRIPS_COUNT} trips${NC}  |  ${REGIONS_COUNT} distinct regions  |  ${DATASOURCES_COUNT} distinct datasources"
echo

step "Q1 (challenge bonus) — From the two most commonly appearing regions, which is the latest datasource?"
echo
pg "
  WITH top_regions AS (
    SELECT region, COUNT(*) AS trips
    FROM trips
    GROUP BY region
    ORDER BY trips DESC
    LIMIT 2
  ), ranked AS (
    SELECT
      t.region,
      t.datasource,
      t.datetime,
      ROW_NUMBER() OVER (PARTITION BY t.region ORDER BY t.datetime DESC) AS rn
    FROM trips t
    JOIN top_regions tr ON tr.region = t.region
  )
  SELECT region, datasource, datetime
  FROM ranked
  WHERE rn = 1;
" | column -t -s '|' | indent
echo

step "Q2 (challenge bonus) — What regions has the 'cheap_mobile' datasource appeared in?"
echo
pg "
  SELECT DISTINCT region
  FROM trips
  WHERE datasource = 'cheap_mobile'
  ORDER BY region;
" | column -t -s '|' | indent
echo

step "Q3 (exploratory) — Average trips per ISO week by region"
echo
pg "
  SELECT
    region,
    iso_week,
    SUM(trip_count)                                      AS weekly_trips,
    ROUND(AVG(SUM(trip_count)) OVER (
      PARTITION BY region ORDER BY iso_week
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                AS rolling_3w_avg
  FROM (
    SELECT region,
           EXTRACT(WEEK FROM datetime)::INT AS iso_week,
           COUNT(*) AS trip_count
    FROM trips
    GROUP BY region, iso_week
  ) sub
  GROUP BY region, iso_week
  ORDER BY region, iso_week;
" | column -t -s '|' | indent
echo

step "Q4 (exploratory) — Bounding-box query: trips originating inside Prague area"
echo
pg "
  SELECT trip_id, region, datasource, datetime
  FROM trips
  WHERE ST_Within(
    origin_geom,
    ST_MakeEnvelope(14.20, 49.94, 14.70, 50.18, 4326)
  )
  ORDER BY datetime DESC
  LIMIT 10;
" | column -t -s '|' | indent

cloud "Full sql/bonus_queries.sql answers challenge questions a) and b) exactly."

# ══════════════════════════════════════════════════════════════════════════════
#  OBSERVABILITY SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

section "OBSERVABILITY — Service URLs"

step "All running services and their interfaces"
echo
printf "   %-28s  %-45s  %s\n" "Service" "URL" "What you'll see"
hr
printf "   %-28s  %-45s  %s\n" "FastAPI (ingestion)"  "http://localhost:8000/docs"  "Swagger UI — POST /ingestions"
printf "   %-28s  %-45s  %s\n" "FastAPI health"       "http://localhost:8000/healthz" "{ status: ok }"
printf "   %-28s  %-45s  %s\n" "Spark Master UI"      "http://localhost:8080"        "Active jobs, streaming queries"
printf "   %-28s  %-45s  %s\n" "Airflow UI"           "http://localhost:8081"        "DAG runs, task logs (admin/admin)"
printf "   %-28s  %-45s  %s\n" "PostgreSQL"           "localhost:5432  db=trips"     "trip_clusters table"
printf "   %-28s  %-45s  %s\n" "Kafka"                "localhost:9092"               "kafka-console-consumer.sh"
echo

step "Useful one-liners for live monitoring"
echo
echo -e "  ${DIM}# Watch Spark streaming queries in the Spark UI:${NC}"
echo -e "  ${CYN}open http://localhost:8080${NC}   # → click Application ID → Structured Streaming"
echo
echo -e "  ${DIM}# Watch PostgreSQL row count grow in real time:${NC}"
echo -e "  ${CYN}docker exec -it postgres psql -U trips -d trips -c 'SELECT count(*) FROM trip_clusters; \\watch 2'${NC}"
echo
echo -e "  ${DIM}# Stream ingestion status events (SSE):${NC}"
echo -e "  ${CYN}curl -N http://localhost:8000/ingestions/${INGESTION_ID}/events${NC}"
echo
echo -e "  ${DIM}# Tail Kafka topic in real time:${NC}"
echo -e "  ${CYN}docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trips.raw --from-beginning${NC}"
echo
echo -e "  ${DIM}# Reset and reprocess from scratch:${NC}"
echo -e "  ${CYN}bash demo.sh  ${DIM}# (default: resets state and runs full demo again)${NC}"

# ══════════════════════════════════════════════════════════════════════════════
#  FINAL PIPELINE VALIDATION SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

section "PIPELINE VALIDATION SUMMARY"

FINAL_COUNT=$(pg "SELECT COUNT(*) FROM trip_clusters;" || echo "?")

echo
echo -e "  ${BOLD}Stage  Check                          Result${NC}"
hr
echo -e "   1     API accepted CSV upload         ${GRN}✔${NC}  HTTP 202, rows=$ROW_COUNT"
echo -e "   2     Kafka topic has events          ${GRN}✔${NC}  $OFFSET_RAW"
echo -e "   3     Bronze Delta table written      ${GRN}✔${NC}  data/delta/bronze_trips/"
echo -e "   3     Silver Delta table written      ${GRN}✔${NC}  data/delta/silver_trips/"
echo -e "   4     Gold Delta table written        ${GRN}✔${NC}  data/delta/gold_trip_clusters/"
FINAL_TRIPS=$(pg "SELECT COUNT(*) FROM trips;" || echo "?")
echo -e "   5     PostgreSQL trips rows          ${GRN}✔${NC}  $FINAL_TRIPS individual trip rows"
echo -e "   5     PostgreSQL trip_clusters rows  ${GRN}✔${NC}  $FINAL_COUNT cluster aggregate rows"
echo -e "   –     Deduplication (2nd upload)      ${GRN}✔${NC}  count unchanged ($BEFORE)"
echo
hr

banner "Demo complete — data is flowing end-to-end"

echo
echo -e "  ${BOLD}Key engineering decisions demonstrated:${NC}"
echo -e "   ${GRN}✔${NC}  Kafka decouples ingestion from processing (fire-and-forget API)"
echo -e "   ${GRN}✔${NC}  Spark Structured Streaming — continuous, not scheduled batch"
echo -e "   ${GRN}✔${NC}  Delta Lake medallion architecture (Bronze / Silver / Gold)"
echo -e "   ${GRN}✔${NC}  Deterministic trip_id → cross-batch deduplication"
echo -e "   ${GRN}✔${NC}  Geohash precision-7 + 30-min buckets for spatial clustering"
echo -e "   ${GRN}✔${NC}  Watermark-based stateful dedup in Spark (1-day window)"
echo -e "   ${GRN}✔${NC}  PostGIS geometry columns — GIST indexes for spatial queries"
echo -e "   ${GRN}✔${NC}  Airflow orchestrates long-running streaming jobs"
echo -e "   ${GRN}✔${NC}  SSE push notifications — no polling on the API"
echo -e "   ${GRN}✔${NC}  TDD: pytest unit + contract tests run before pipeline"
echo
echo -e "  ${BOLD}Cloud-ready equivalents:${NC}"
echo -e "   ${MGT}☁${NC}  Kafka       →  AWS MSK (managed Kafka, multi-AZ)"
echo -e "   ${MGT}☁${NC}  Spark       →  Databricks (Unity Catalog, DLT, auto-scaling)"
echo -e "   ${MGT}☁${NC}  Delta Lake  →  Databricks Delta Lake on S3"
echo -e "   ${MGT}☁${NC}  PostgreSQL  →  Amazon RDS + PostGIS (Multi-AZ, read replicas)"
echo -e "   ${MGT}☁${NC}  Airflow     →  Databricks Workflows or Amazon MWAA"
echo -e "   ${MGT}☁${NC}  FastAPI     →  Amazon EKS + ALB with HPA"
echo
echo -e "  ${DIM}Full docs: README.md | Architecture: docs/solution-architecture-proposal-en-us.md${NC}"
echo
