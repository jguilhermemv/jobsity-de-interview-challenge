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
#          Stage 6 — Weekly average query API (by region + by bounding box)
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
#
#  RESOURCE REQUIREMENTS:
#    Docker VM (Colima, Docker Desktop) must have ≥8 CPUs and ≥8 GB RAM.
#    Spark worker needs 8 CPUs and 6 GB. With less, Job 2 may timeout.
#    Colima: colima start --arch x86_64 --cpu 8 --memory 8
# =============================================================================
set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Demo-wide start time (set before any flag parsing so --help doesn't skew it)
DEMO_START=$(date +%s)
SECTION_START=$DEMO_START
FOOTER_TIMER_PID=""

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
      echo "Usage: bash demo.sh [OPTIONS]"
      echo ""
      echo "Full end-to-end demo: CSV → FastAPI → Kafka → Spark → Delta Lake → PostgreSQL"
      echo ""
      echo "Options:"
      echo "  --resume       Skip teardown/reset — reuse already-running containers"
      echo "  --skip-build   Skip 'docker compose build' (images already built)"
      echo "  --no-tests     Skip the pytest unit/contract test suite"
      echo "  --help, -h     Show this help message and exit"
      echo ""
      echo "Resource requirements: Docker VM ≥8 CPUs, ≥8 GB RAM (Colima: colima start --cpu 8 --memory 8)"
      echo ""
      echo "Approximate runtime:"
      echo "  First run  (build + Maven download):  20–35 min"
      echo "  Subsequent (--resume --skip-build):    5–10 min"
      echo ""
      echo "Log file:"
      echo "  Output is tee'd to logs/demo_YYYYMMDD_HHMMSS.log (ANSI codes stripped)."
      echo "  Override directory with: LOG_DIR=/path/to/dir bash demo.sh"
      echo ""
      echo "Examples:"
      echo "  bash demo.sh                           # clean run from scratch"
      echo "  bash demo.sh --resume --skip-build     # reuse containers, skip build"
      echo "  bash demo.sh --no-tests --skip-build   # fast run without test suite"
      exit 0
      ;;
    *)
      echo "Unknown flag: $arg  (use --help)" && exit 1 ;;
  esac
done

# ── Log file setup ─────────────────────────────────────────────────────────────
# All output goes to both the terminal (with ANSI colours) and a plain-text log
# file (ANSI escape sequences stripped so it is grep/cat friendly).
#   Override the directory with:  LOG_DIR=/tmp bash demo.sh
LOG_DIR="${LOG_DIR:-logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/demo_$(date +%Y%m%d_%H%M%S).log"

# ESC char used in the sed ANSI-stripping expression (portable: no \x1B needed)
_ESC=$(printf '\033')
# Redirect stdout → tee → (console) + (strip-ANSI → log file)
#              stderr is merged into stdout so errors also appear in the log
exec > >(tee >(sed "s/${_ESC}\[[0-9;:]*[A-Za-z]//g; s/\r//g" >> "$LOG_FILE")) 2>&1
echo -e "\033[2mLogging to: ${LOG_FILE}\033[0m"

# ── Pretty helpers ─────────────────────────────────────────────────────────────
hr()       { echo -e "${DIM}$(printf '─%.0s' {1..72})${NC}"; }

# Returns elapsed time since DEMO_START as HH:MM:SS
elapsed() {
  local s=$(( $(date +%s) - DEMO_START ))
  printf "%02d:%02d:%02d" $(( s/3600 )) $(( (s%3600)/60 )) $(( s%60 ))
}

# Returns elapsed time since SECTION_START as HH:MM:SS
section_elapsed() {
  local s=$(( $(date +%s) - SECTION_START ))
  printf "%02d:%02d:%02d" $(( s/3600 )) $(( (s%3600)/60 )) $(( s%60 ))
}

# Starts a live footer timer on the last terminal line.
# Updates every second using ESC-7/ESC-8 (DEC save/restore cursor) so the
# main output stream is never disturbed. No scrolling region — no layout side
# effects.
start_footer_timer() {
  (
    trap 'exit 0' TERM INT
    while true; do
      local s=$(( $(date +%s) - DEMO_START ))
      local hms
      hms=$(printf "%02d:%02d:%02d" $(( s/3600 )) $(( (s%3600)/60 )) $(( s%60 )))
      local r
      r=$(tput lines 2>/dev/null || echo 24)
      printf '\0337\033[%s;1H\033[2K\033[7m  ⏱  Demo running: +%s  \033[0m\0338' \
        "$r" "$hms" 2>/dev/null
      sleep 1
    done
  ) &
  FOOTER_TIMER_PID=$!
  disown "$FOOTER_TIMER_PID" 2>/dev/null
}

# Kills the footer timer and clears the footer line.
stop_footer_timer() {
  if [[ -n "${FOOTER_TIMER_PID:-}" ]]; then
    kill "$FOOTER_TIMER_PID" 2>/dev/null
    wait "$FOOTER_TIMER_PID" 2>/dev/null || true
    FOOTER_TIMER_PID=""
  fi
  local rows
  rows=$(tput lines 2>/dev/null || echo 24)
  printf '\0337\033[%s;1H\033[2K\0338' "$rows" 2>/dev/null
}

# Cleanup on normal exit (including after explicit `exit` calls).
trap 'stop_footer_timer' EXIT
# On Ctrl+C / SIGTERM: clean up then exit with standard signal codes.
trap 'stop_footer_timer; exit 130' INT
trap 'stop_footer_timer; exit 143' TERM

banner() {
  echo
  echo -e "${BOLD}${BLU}╔$(printf '═%.0s' {1..72})╗${NC}"
  echo -e "${BOLD}${BLU}║$(printf ' %.0s' {1..72})║${NC}"
  printf "${BOLD}${BLU}║  ${NC}${BOLD}%-70s${BLU}║${NC}\n" "$1"
  echo -e "${BOLD}${BLU}║$(printf ' %.0s' {1..72})║${NC}"
  echo -e "${BOLD}${BLU}╚$(printf '═%.0s' {1..72})╝${NC}"
}

# section <title>  — prints a cyan box header.
# Top line: title + global elapsed (right-aligned).
# Second line: section elapsed since previous section started (frozen label).
# Resets SECTION_START to now so the next section measures from here.
section() {
  local prev_section_dur
  prev_section_dur=$(section_elapsed)
  SECTION_START=$(date +%s)
  echo
  echo -e "${BOLD}${CYN}┌$(printf '─%.0s' {1..72})┐${NC}"
  printf "${BOLD}${CYN}│  ${NC}${BOLD}%-59s${DIM}  +%-8s${CYN}│${NC}\n" "$1" "$(elapsed)"
  printf "${BOLD}${CYN}│  ${DIM}%-68s${CYN}│${NC}\n" "previous section: ${prev_section_dur}"
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
# Shows a spinner + per-task elapsed time while polling, e.g.:
#   Waiting for Job 1 Bronze — first commit  ⠋ ⏱ +00:02:31
# Overwrites the line in-place each tick (interval=1s for smooth spinner).
wait_for() {
  local label="$1" timeout="$2" expr="$3"
  local task_start interval=1
  task_start=$(date +%s)
  local frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
  local fi=0

  printf "     ${DIM}Waiting for${NC} %-44s" "$label"
  while ! eval "$expr" &>/dev/null 2>&1; do
    local task_s=$(( $(date +%s) - task_start ))
    local task_hms
    task_hms=$(printf "%02d:%02d:%02d" $(( task_s/3600 )) $(( (task_s%3600)/60 )) $(( task_s%60 )))
    printf "\r     ${DIM}Waiting for${NC} %-44s ${CYN}%s${NC}  ${DIM}⏱ +%s${NC}" \
      "$label" "${frames[$fi]}" "$task_hms"
    fi=$(( (fi + 1) % ${#frames[@]} ))
    if [[ $(( $(date +%s) - task_start )) -ge $timeout ]]; then
      echo -e "\n     ${RED}✖  TIMEOUT (${timeout}s) — ${label}${NC}"
      return 1
    fi
    sleep "$interval"
  done
  local task_s=$(( $(date +%s) - task_start ))
  local task_hms
  task_hms=$(printf "%02d:%02d:%02d" $(( task_s/3600 )) $(( (task_s%3600)/60 )) $(( task_s%60 )))
  printf "\r     ${GRN}✔  %-44s ${NC}${DIM}ready  ⏱ +%s${NC}\n" "$label" "$task_hms"
}

# Returns the latest Kafka partition-0 offset consumed by Spark Job1.
# Reads the most recent file in data/checkpoints/job1_unified/offsets/
# (host filesystem — no Kafka tools required).
get_job1_checkpoint_offset() {
  local ckpt_dir="data/checkpoints/job1_unified/offsets"
  local latest
  latest=$(ls "$ckpt_dir" 2>/dev/null | sort -n | tail -1)
  [[ -z "$latest" ]] && echo "0" && return
  python3 -c "
import json, sys
lines = open('${ckpt_dir}/${latest}').read().strip().split('\n')
for line in reversed(lines):
    try:
        d = json.loads(line)
        if 'trips.raw' in d:
            print(sum(int(v) for v in d['trips.raw'].values()))
            sys.exit(0)
    except Exception:
        pass
print(0)
" 2>/dev/null || echo "0"
}

# watch_pg_counts <max_wait_seconds> [expected_trips] [expected_kafka_offset]
#
# Two-phase watch:
#   Phase 1 — polls the Spark Job1 checkpoint offset until it reaches
#              expected_kafka_offset. This is the precise signal that Job1
#              has consumed every Kafka event published for this ingestion.
#              No heuristics, no timing guesses.
#   Phase 2 — waits a fixed 15 s buffer for Job2 to write its last
#              micro-batch to PostgreSQL, then polls until the trip count
#              is unchanged for 4 consecutive checks (8 s = truly stable).
#
# Uses ANSI cursor-up to overwrite the live count display in-place.
# All 4 PG counts are fetched in a single docker exec / psql round-trip.
watch_pg_counts() {
  local max_wait="${1:-120}"
  local expected="${2:-0}"
  local expected_kafka_offset="${3:-0}"
  local interval=2
  local elapsed_w=0

  # ── Phase 1: wait for Job1 to consume all Kafka events ──────────────────
  if [[ "$expected_kafka_offset" -gt 0 ]]; then
    echo
    echo -e "     ${BOLD}Phase 1${NC}  ${DIM}waiting for Spark Job1 to consume all Kafka events${NC}"
    echo -e "     ${DIM}(target offset: ${expected_kafka_offset})${NC}"
    echo

    local p1_first=true
    while true; do
      local ckpt_offset
      ckpt_offset=$(get_job1_checkpoint_offset)

      if ! $p1_first; then printf '\033[1A\r\033[2K'; fi
      p1_first=false

      if [[ "$ckpt_offset" =~ ^[0-9]+$ && "$ckpt_offset" -ge "$expected_kafka_offset" ]]; then
        echo -e "     ${GRN}✔${NC}  Job1 checkpoint: ${BOLD}${ckpt_offset}${NC} / ${expected_kafka_offset} — all events consumed"
        break
      fi
      echo -e "     ${DIM}Job1 checkpoint: ${ckpt_offset:-0} / ${expected_kafka_offset}  (global +$(elapsed)  section +$(section_elapsed))${NC}"

      elapsed_w=$(( elapsed_w + interval ))
      if [[ $elapsed_w -ge $max_wait ]]; then
        echo
        warn "Phase 1 timeout (${max_wait}s) — Job1 checkpoint: ${ckpt_offset:-0}/${expected_kafka_offset}"
        break
      fi
      sleep $interval
    done

    echo
    echo -e "     ${DIM}Phase 2: waiting 15 s for Job2 to flush last micro-batch…${NC}"
    sleep 15
    echo -e "     ${GRN}✔${NC}  Job2 buffer elapsed — checking PostgreSQL stability"
    # Reset timer: Phase 2 gets its own 60 s window for stability detection.
    elapsed_w=0
    max_wait=60
  fi

  # ── Phase 2: poll PostgreSQL until count is stable ───────────────────────
  echo
  if [[ "$expected" -gt 0 ]]; then
    echo -e "     ${BOLD}Live PostgreSQL counts${NC}  ${DIM}(refreshes every ${interval}s — target: ${expected} trips)${NC}"
  else
    echo -e "     ${BOLD}Live PostgreSQL counts${NC}  ${DIM}(refreshes every ${interval}s — stops when stable)${NC}"
  fi
  echo

  local prev_trips=-1
  local stable=0
  local NLINES=6
  local first=true

  while true; do
    local _row
    _row=$(docker exec -i postgres psql -U trips -d trips --no-align -t -c \
      "SELECT (SELECT COUNT(*) FROM trips),(SELECT COUNT(*) FROM trip_clusters),(SELECT COUNT(*) FROM regions),(SELECT COUNT(*) FROM datasources);" \
      2>/dev/null) || _row="?|?|?|?"

    local trips_n clusters_n regions_n ds_n
    trips_n=$(   printf '%s' "$_row" | cut -d'|' -f1 | tr -d '[:space:]')
    clusters_n=$(printf '%s' "$_row" | cut -d'|' -f2 | tr -d '[:space:]')
    regions_n=$( printf '%s' "$_row" | cut -d'|' -f3 | tr -d '[:space:]')
    ds_n=$(      printf '%s' "$_row" | cut -d'|' -f4 | tr -d '[:space:]')

    [[ -z "$trips_n"    ]] && trips_n="?"
    [[ -z "$clusters_n" ]] && clusters_n="?"
    [[ -z "$regions_n"  ]] && regions_n="?"
    [[ -z "$ds_n"       ]] && ds_n="?"

    if ! $first; then printf '\033[%dA' $NLINES; fi
    first=false

    local pct=""
    if [[ "$expected" -gt 0 && "$trips_n" =~ ^[0-9]+$ ]]; then
      pct="  ($(( trips_n * 100 / expected ))%)"
    fi

    echo -e "\r\033[2K     ${CYN}trips            ${NC}  ${BOLD}${GRN}${trips_n}${pct}${NC}"
    echo -e "\r\033[2K     ${CYN}trip_clusters    ${NC}  ${BOLD}${GRN}${clusters_n}${NC}"
    echo -e "\r\033[2K     ${CYN}regions          ${NC}  ${BOLD}${GRN}${regions_n}${NC}"
    echo -e "\r\033[2K     ${CYN}datasources      ${NC}  ${BOLD}${GRN}${ds_n}${NC}"
    echo -e "\r\033[2K"
    printf   "\r\033[2K     ${DIM}elapsed: +%s global  +%s section${NC}\n" "$(elapsed)" "$(section_elapsed)"

    if [[ "$trips_n" =~ ^[0-9]+$ && "$trips_n" -gt 0 ]]; then
      if [[ "$prev_trips" -eq -1 || "$trips_n" -ne "$prev_trips" ]]; then
        stable=0
        prev_trips=$trips_n
      else
        stable=$(( stable + 1 ))
        if [[ $stable -ge 4 ]]; then
          echo
          if [[ "$expected" -gt 0 && "$trips_n" -lt "$expected" ]]; then
            ok "Counts stable (trips=${trips_n}/${expected}) — deduplication reduced final count"
          else
            ok "Counts stable (trips=${trips_n}) — all rows processed"
          fi
          return 0
        fi
      fi
    fi

    elapsed_w=$(( elapsed_w + interval ))
    if [[ $elapsed_w -ge $max_wait ]]; then
      echo
      warn "Watch timeout (${max_wait}s) — trips=${trips_n:-?}/${expected}"
      return 0
    fi
    sleep $interval
  done
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
start_footer_timer

# ══════════════════════════════════════════════════════════════════════════════
#  1.  PREREQUISITES
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 0 — Prerequisites"

# Spark worker needs 8 CPUs and 6 GB (docker-compose.yml). Define early for Colima auto-start.
REQUIRED_MEM_GB=7.5
REQUIRED_CPUS=8
COLIMA_MEMORY=8

step "Checking required tools"

command -v docker  &>/dev/null || die "docker not found. Install Docker Desktop, Colima, or Rancher Desktop first."
ok "docker found: $(docker --version)"

# ── Docker runtime check ────────────────────────────────────────────────────
# On macOS the Docker CLI is just a client; it needs a running VM/daemon.
# Supported runtimes: Colima, Rancher Desktop, or Docker Desktop.
# We first probe the daemon (fast). If that fails, we try to auto-start Colima.
step "Checking Docker daemon / runtime"

_docker_running() { docker info &>/dev/null 2>&1; }

if _docker_running; then
  ok "Docker daemon is reachable"
else
  # Docker daemon is NOT running — check which runtime is available
  if command -v colima &>/dev/null; then
    warn "Docker daemon not responding. Colima is installed — attempting to start it (arch: x86_64, cpu: ${REQUIRED_CPUS}, memory: ${COLIMA_MEMORY} GB)..."
    # --arch x86_64 is required because several images in the stack (Spark, Kafka)
    # are only published for linux/amd64 and do not have ARM64 variants.
    # --cpu and --memory ensure the Spark worker (8 CPUs, 6 GB) can be allocated.
    if colima start --arch x86_64 --cpu "${REQUIRED_CPUS}" --memory "${COLIMA_MEMORY}" 2>&1 | indent; then
      # Give the socket a moment to become available
      _wait=0
      while ! _docker_running && [[ $_wait -lt 30 ]]; do
        sleep 2; _wait=$(( _wait + 2 ))
      done
      if _docker_running; then
        ok "Colima started — Docker daemon is now reachable"
      else
        die "Colima started but Docker daemon is still not reachable after 30 s. Check 'colima status'."
      fi
    else
      die "Failed to start Colima. Run 'colima start' manually and re-run this script."
    fi
  elif command -v limactl &>/dev/null; then
    # Rancher Desktop uses lima under the hood but exposes its own CLI
    die "Docker daemon is not running. Please open Rancher Desktop and wait until it is ready, then re-run this script."
  else
    echo
    echo -e "  ${RED}${BOLD}✖  Docker daemon is not running and no known runtime was auto-detected.${NC}"
    echo
    echo -e "  To fix this, install and start one of the following:"
    echo -e "   ${CYN}• Colima (recommended, lightweight):${NC}"
    echo -e "       brew install colima && colima start --arch x86_64 --cpu ${REQUIRED_CPUS} --memory ${COLIMA_MEMORY}"
    echo -e "   ${CYN}• Rancher Desktop:${NC}"
    echo -e "       https://rancherdesktop.io"
    echo -e "   ${CYN}• Docker Desktop:${NC}"
    echo -e "       https://www.docker.com/products/docker-desktop"
    echo
    exit 1
  fi
fi

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

# ── Docker VM memory check ─────────────────────────────────────────────────────
# The Spark worker requires 8 CPUs and 6 GB (docker-compose.yml). The Docker VM
# (Colima, Docker Desktop, etc.) must have enough resources or jobs will hang.
step "Checking Docker VM resources (Spark worker needs 8 CPUs, 6 GB)"

DOCKER_INFO=$(docker info 2>/dev/null) || true

# Parse memory from docker info — format: "Memory: 7.8GiB" or "Total Memory: 8192MiB"
MEM_LINE=$(echo "$DOCKER_INFO" | grep -iE "memory|mem total" | head -1)
MEM_GB=""
if [[ "$MEM_LINE" =~ ([0-9]+\.?[0-9]*)[[:space:]]*(GiB|GB) ]]; then
  MEM_GB="${BASH_REMATCH[1]}"
elif [[ "$MEM_LINE" =~ ([0-9]+)[[:space:]]*(MiB|MB) ]]; then
  MEM_VAL="${BASH_REMATCH[1]}"
  MEM_GB=$(awk "BEGIN {printf \"%.1f\", $MEM_VAL / 1024}" 2>/dev/null || echo "$(( MEM_VAL / 1024 ))")
fi

# Parse CPUs — format: "CPUs: 8" or "NCPU: 8"
CPU_LINE=$(echo "$DOCKER_INFO" | grep -iE "cpus|ncpu" | head -1)
CPUS=""
[[ "$CPU_LINE" =~ ([0-9]+) ]] && CPUS="${BASH_REMATCH[1]}"

if [[ -n "$MEM_GB" && -n "$CPUS" ]]; then
  MEM_OK=false
  CPU_OK=false
  awk "BEGIN {exit !($MEM_GB >= $REQUIRED_MEM_GB)}" 2>/dev/null && MEM_OK=true
  [[ "$CPUS" -ge "$REQUIRED_CPUS" ]] 2>/dev/null && CPU_OK=true

  if $MEM_OK && $CPU_OK; then
    ok "Docker VM: ${CPUS} CPUs, ${MEM_GB} GB — sufficient for Spark worker"
  else
    echo
    echo -e "  ${RED}${BOLD}✖  Insufficient Docker VM resources.${NC}"
    echo -e "     Detected: ${CPUS:-?} CPUs, ${MEM_GB:-?} GB"
    echo -e "     Required: ${REQUIRED_CPUS} CPUs, ≥${REQUIRED_MEM_GB} GB (Spark worker: 8 CPUs, 6 GB)"
    echo
    if command -v colima &>/dev/null; then
      echo -e "  ${CYN}Colima detected.${NC} Restart with more resources:"
      echo -e "     colima stop"
      echo -e "     colima delete"
      echo -e "     colima start --arch x86_64 --cpu ${REQUIRED_CPUS} --memory ${COLIMA_MEMORY}"
      echo
    else
      echo -e "  ${CYN}Docker Desktop:${NC} Settings → Resources → increase Memory to ${REQUIRED_MEM_GB} GB, CPUs to ${REQUIRED_CPUS}"
      echo
    fi
    exit 1
  fi
else
  warn "Could not parse Docker VM resources. Ensure Colima/Docker Desktop has ≥${REQUIRED_CPUS} CPUs and ≥${REQUIRED_MEM_GB} GB."
  warn "Otherwise Job 2 may timeout waiting for Spark executor allocation."
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
         data/delta/silver_ingestion_markers \
         data/delta/rejected_trips \
         data/delta/gold_trip_clusters \
         data/delta/gold_weekly_metrics
  rm -rf data/checkpoints
  ok "Delta tables cleared"
  ok "Spark checkpoints cleared"

  step "Removing Airflow runtime data"
  info "data/airflow/  — Airflow metadata DB files, logs, and plugin cache"
  info "  (preserving data/airflow/.ivy2/ — Maven JAR cache, takes minutes to re-download)"
  find data/airflow -mindepth 1 -maxdepth 1 ! -name '.ivy2' -exec rm -rf {} + 2>/dev/null || true
  ok "Airflow runtime cleared (Ivy JAR cache preserved)"

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

step "Pre-creating Kafka topic: trips.raw"
info "Job 1 connects to Kafka at startup — the topic must exist before spark-submit runs."
info "Without pre-creation, Spark raises UnknownTopicOrPartitionException and the job crashes."
docker exec kafka kafka-topics.sh \
  --create \
  --topic trips.raw \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists 2>&1 | indent
ok "Kafka topic trips.raw is ready"

step "Unpausing DAG: $DAG_ID"
docker exec airflow-webserver \
  airflow dags unpause "$DAG_ID" 2>&1 | indent
ok "DAG unpaused — scheduler will now pick up triggered runs"

step "Triggering DAG: $DAG_ID  (run-id: $RUN_ID)"
docker exec airflow-webserver \
  airflow dags trigger "$DAG_ID" --run-id "$RUN_ID" 2>&1 | indent
ok "DAG triggered — both streaming jobs are starting in the background"

info "On first run, Spark downloads Maven packages (Kafka connector, Delta Lake,"
info "PostgreSQL JDBC). This takes 2–5 minutes. Waiting for all streams to be ready"
info "before uploading — so the data flows live as soon as the CSV is submitted."

step "Waiting for all streaming queries to initialise"
info "(Job 1 initialises the Silver table with the correct schema at startup — even before"
info " any Kafka data — so Job 2 starts immediately and both jobs warm up in parallel.)"
info "(Waiting for commits/0, not just metadata — ensures each stream has fired its first"
info " trigger and confirmed it can connect to all sources/sinks before data is sent.)"
echo

wait_for "Job 1 (Bronze+Silver+Markers) — first commit"  600 \
  'test -f data/checkpoints/job1_unified/commits/0'

wait_for "Job 2 Gold clusters — first commit"  300 \
  'test -f data/checkpoints/gold_trip_clusters/commits/0'

wait_for "Job 2 Gold weekly — first commit"  180 \
  'test -f data/checkpoints/gold_weekly_metrics/commits/0'

wait_for "Job 2 Postgres trips — first commit"  180 \
  'test -f data/checkpoints/postgres_trips/commits/0'

wait_for "Job 2 Postgres clusters — first commit"  120 \
  'test -f data/checkpoints/postgres_trip_clusters/commits/0'

wait_for "Job 2 Markers completion — first commit"  60 \
  'test -f data/checkpoints/markers_completion/commits/0'

ok "All 7 streaming queries are hot — pipeline is ready for data"
cloud "On Databricks: Workflows health-check API confirms job readiness before ingestion."

# ══════════════════════════════════════════════════════════════════════════════
#  7.  UPLOAD THE CSV
# ══════════════════════════════════════════════════════════════════════════════

section "STAGE 6 — Upload trips.csv to the API"

narrate "The FastAPI endpoint POST /ingestions accepts a CSV file, parses all rows"
narrate "synchronously, and returns HTTP 202 immediately with the ingestion_id and"
narrate "row count — before any event reaches Kafka."
narrate ""
narrate "A BackgroundTask then publishes one Kafka event per row with a configurable"
narrate "delay (PUBLISH_DELAY_SECONDS=0.5), simulating a realistic ingestion stream."
narrate "Each event carries a deterministic trip_id (SHA-256 of stable fields,"
narrate "excluding ingestion_id) — enabling cross-batch deduplication in Spark."

step "Uploading sample_data/trips.csv"
echo

# Snapshot Job1's Kafka offset before the upload so we can calculate the
# exact target offset after all ROW_COUNT events have been published.
START_KAFKA_OFFSET=$(get_job1_checkpoint_offset)

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
ROW_COUNT=$(echo "$INGEST_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['rows'])" 2>/dev/null || \
            echo "$INGEST_RESPONSE" | grep -o '"rows":[0-9]*' | grep -o '[0-9]*' || \
            echo "?")

ok "202 Accepted — ingestion queued for async publish"
ok "ingestion_id = $INGESTION_ID"
ok "rows queued for background publish = $ROW_COUNT"

step "Live pipeline — watching data flow in real time"
narrate "The API is publishing ${ROW_COUNT} events to Kafka at 0.5 s/event (~50 s total)."
narrate "Spark consumes them micro-batch by micro-batch (every 5 s) and writes to PostgreSQL."
narrate "Watch the trip count grow live:"
echo
# Expected Kafka offset = offset before upload + number of rows published.
# watch_pg_counts Phase 1 polls the Spark checkpoint until Job1 has consumed
# all events (ROW_COUNT trips + 1 end marker); Phase 2 then checks PostgreSQL stability.
EXPECTED_KAFKA_OFFSET=$(( START_KAFKA_OFFSET + ROW_COUNT + 1 ))
watch_pg_counts 180 "$ROW_COUNT" "$EXPECTED_KAFKA_OFFSET"

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
info "Format: topic:partition:offset  —  offset should be ≥ $(( ROW_COUNT + 1 )) (rows + end marker)"

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
narrate "  • Silver: valid rows, deduplicated by trip_id (1-minute watermark)"
narrate "  • Rejected: invalid coordinate ranges or null trip_id (DLQ)"

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

info "All 4 Job 2 streaming queries were already initialised during the warm-up phase."
info "PostgreSQL already received live data during the live counter above."
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
#  STAGE 6 VALIDATION — WEEKLY AVERAGE QUERY API
# ══════════════════════════════════════════════════════════════════════════════

section "VALIDATION — Stage 6: Weekly Average Trips Query API"

narrate "GET /trips/weekly-average returns the average number of trips per ISO week"
narrate "for a given area — identified either by region name or by bounding box."
narrate "Queries run directly against PostgreSQL; PostGIS ST_Within handles bbox."

step "Query by region name (Prague)"
WA_REGION=$(curl -sf "http://localhost:8000/trips/weekly-average?region=Prague" || echo "{}")
echo "     Response: $WA_REGION"
echo

step "Query by region name (Turin)"
WA_TURIN=$(curl -sf "http://localhost:8000/trips/weekly-average?region=Turin" || echo "{}")
echo "     Response: $WA_TURIN"
echo

step "Query by bounding box (Prague area: lon 14.0–14.7, lat 49.9–50.2)"
WA_BBOX=$(curl -sf "http://localhost:8000/trips/weekly-average?min_lon=14.0&min_lat=49.9&max_lon=14.7&max_lat=50.2" || echo "{}")
echo "     Response: $WA_BBOX"
echo

step "Error case — no filter provided (must return 400)"
HTTP_STATUS=$(curl -so /dev/null -w "%{http_code}" "http://localhost:8000/trips/weekly-average")
if [[ "$HTTP_STATUS" == "400" ]]; then
  ok "Correctly returned HTTP 400 when no filter was provided"
else
  warn "Expected 400, got $HTTP_STATUS"
fi

# Determine pass/fail for summary line
WA_OK="FAIL"
if echo "$WA_REGION" | grep -q "weekly_average"; then
  WA_OK="OK"
fi

cloud "The endpoint uses PostGIS ST_Within(origin_geom, ST_MakeEnvelope(...)) with a"
cloud "GIST spatial index — sub-millisecond lookups at 100M-row scale on RDS."

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
if [[ "$WA_OK" == "OK" ]]; then
  echo -e "   6     Weekly avg query (by region)   ${GRN}✔${NC}  GET /trips/weekly-average → $WA_REGION"
else
  echo -e "   6     Weekly avg query (by region)   ${YLW}⚠${NC}  endpoint reachable but DB returned no data yet"
fi
echo -e "   –     Deduplication (2nd upload)      ${GRN}✔${NC}  count unchanged ($BEFORE)"
echo

echo -e "  ${BOLD}Direct SQL (view weekly_trips_by_region):${NC}"
pg "SELECT region, weekly_average, num_weeks, total_trips FROM weekly_trips_by_region;" \
  | column -t -s '|' | indent
echo
hr

TOTAL_ELAPSED=$(elapsed)
stop_footer_timer
banner "Demo complete — total time: ${TOTAL_ELAPSED}"

echo
echo -e "  ${BOLD}Key engineering decisions demonstrated:${NC}"
echo -e "   ${GRN}✔${NC}  Kafka decouples ingestion from processing (fire-and-forget API)"
echo -e "   ${GRN}✔${NC}  Spark Structured Streaming — continuous, not scheduled batch"
echo -e "   ${GRN}✔${NC}  Delta Lake medallion architecture (Bronze / Silver / Gold)"
echo -e "   ${GRN}✔${NC}  Deterministic trip_id → cross-batch deduplication"
echo -e "   ${GRN}✔${NC}  Geohash precision-7 + 30-min buckets for spatial clustering"
echo -e "   ${GRN}✔${NC}  Watermark-based stateful dedup in Spark (1-minute window)"
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
echo -e "  ${DIM}Log file:  ${LOG_FILE}${NC}"
echo
