#!/usr/bin/env bash
set -Eeuo pipefail

trap 'echo "Script failed at line $LINENO while running: $BASH_COMMAND" >&2' ERR

API_URL="${API_URL:-http://localhost:8080}"
DB_CONTAINER="${DB_CONTAINER:-forgequeue-postgres}"
DB_USER="${DB_USER:-forgequeue}"
DB_NAME="${DB_NAME:-forgequeue}"
DB_URL="${FORGEQUEUE_DATABASE_URL:-postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable}"

WORKER_PROCESSES="${WORKER_PROCESSES:-5}"
JOBS="${JOBS:-5}"

RESULTS_DIR="load-results/crash-reclaim-$(date +%Y%m%d-%H%M%S)"
RAW_DIR="$RESULTS_DIR/raw"
mkdir -p "$RAW_DIR"

SUMMARY_FILE="$RESULTS_DIR/summary.md"
WORKER_BIN="$RESULTS_DIR/forgequeue-worker"

echo "Building worker binary..."
go build -o "$WORKER_BIN" ./cmd/worker

require_api() {
  if ! curl -fsS "$API_URL/healthz" >/dev/null; then
    echo "API is not reachable at $API_URL"
    exit 1
  fi
}

cleanup_existing_workers() {
  echo "Cleaning up old worker processes..."

  for port in $(seq 9091 9105); do
    sudo lsof -tiTCP:"$port" -sTCP:LISTEN | xargs -r kill -9 || true
  done

  pkill -9 -f 'go run ./cmd/worker' 2>/dev/null || true
  pkill -9 -f 'make run-worker' 2>/dev/null || true
  pkill -9 -f '/tmp/go-build.*/exe/worker' 2>/dev/null || true
  pkill -9 -f 'forgequeue-worker' 2>/dev/null || true

  sleep 1
}

clear_jobs() {
  echo "Clearing jobs table..."
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "TRUNCATE jobs;" >/dev/null
}

db_query() {
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -A -c "$1"
}

status_count() {
  local status="$1"
  db_query "SELECT COUNT(*) FROM jobs WHERE status = '$status';"
}

wait_until_running_count() {
  local expected="$1"

  while true; do
    local running
    running="$(status_count running)"

    echo "running=$running"

    if [[ "$running" == "$expected" ]]; then
      break
    fi

    sleep 1
  done
}

wait_until_final() {
  while true; do
    local pending
    local running
    local completed
    local dead

    pending="$(status_count pending)"
    running="$(status_count running)"
    completed="$(status_count completed)"
    dead="$(status_count dead)"

    echo "pending=$pending running=$running completed=$completed dead=$dead"

    if [[ "$pending" == "0" && "$running" == "0" && "$completed" == "$JOBS" ]]; then
      break
    fi

    sleep 5
  done
}

submit_slow_jobs() {
  echo "Submitting $JOBS slow jobs..."

  for i in $(seq 1 "$JOBS"); do
    curl -fsS -X POST "$API_URL/jobs" \
      -H "Content-Type: application/json" \
      -d "{\"kind\":\"slow_test_job\",\"payload\":{\"index\":$i},\"max_retries\":3}" >/dev/null
  done
}

start_workers() {
  echo "Starting $WORKER_PROCESSES separate worker processes..."

  WORKER_PIDS=()

  for i in $(seq 1 "$WORKER_PROCESSES"); do
    local port
    port=$((9090 + i))

    local log
    log="$RAW_DIR/worker_${i}.log"

    setsid env \
      FORGEQUEUE_DATABASE_URL="$DB_URL" \
      FORGEQUEUE_WORKER_COUNT=1 \
      FORGEQUEUE_WORKER_METRICS_ADDR=":$port" \
      "$WORKER_BIN" > "$log" 2>&1 &

    WORKER_PIDS+=("$!")

    echo "worker_process=$i pid=${WORKER_PIDS[-1]} metrics_port=$port log=$log"
  done
}

cleanup_workers() {
  for pid in "${WORKER_PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -TERM "-$pid" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL "-$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
}

require_api
cleanup_existing_workers
clear_jobs
start_workers

trap cleanup_workers EXIT

sleep 3

submit_slow_jobs

echo "Waiting until all jobs are running..."
wait_until_running_count "$JOBS"

KILLED_PID="${WORKER_PIDS[0]}"
KILLED_AT="$(date +%s)"

echo "Killing one worker process with SIGKILL: pid=$KILLED_PID"
kill -KILL "-$KILLED_PID" >/dev/null 2>&1 || true
wait "$KILLED_PID" >/dev/null 2>&1 || true

echo "Waiting for remaining workers to reclaim and finish all jobs..."
wait_until_final

FINISHED_AT="$(date +%s)"
TOTAL_TIME=$((FINISHED_AT - KILLED_AT))

COMPLETED="$(status_count completed)"
DEAD="$(status_count dead)"
PENDING="$(status_count pending)"
RUNNING="$(status_count running)"
RETRIED_OR_RECLAIMED="$(db_query "SELECT COUNT(*) FROM jobs WHERE retry_count > 0;")"

cat > "$SUMMARY_FILE" <<EOF
# ForgeQueue Crash/Reclaim Test

Date: $(date)

## Scenario

- Started $WORKER_PROCESSES separate worker processes
- Each worker process had \`FORGEQUEUE_WORKER_COUNT=1\`
- Submitted $JOBS \`slow_test_job\` jobs
- Waited until all jobs were running
- Killed one worker process with \`kill -9\`
- Waited for remaining workers to reclaim and complete all jobs

## Results

| Worker Processes | Jobs | Killed Workers | Time After Kill Until Completion | Completed | Dead | Pending | Running | Jobs With Retry/Reclaim Count |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| $WORKER_PROCESSES | $JOBS | 1 | ${TOTAL_TIME}s | $COMPLETED | $DEAD | $PENDING | $RUNNING | $RETRIED_OR_RECLAIMED |

## Interpretation

The killed worker left one job in \`running\` state with an active lease. After the lease expired, a surviving worker reclaimed the job and completed it.

The expected successful result is:

- all jobs completed,
- no jobs dead,
- no jobs stuck in pending or running,
- at least one job has \`retry_count > 0\`, showing it was reclaimed after lease expiry.
EOF

echo
echo "Crash/reclaim scenario complete."
echo "Summary file:"
echo "$SUMMARY_FILE"
echo
cat "$SUMMARY_FILE"
