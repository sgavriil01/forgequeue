#!/usr/bin/env bash
set -Eeuo pipefail

DB_URL="${FORGEQUEUE_DATABASE_URL:-postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable}"
API_URL="${API_URL:-http://localhost:8080}"
JOBS="${JOBS:-20}"

API_PID=""
WORKER_PID=""

cleanup() {
  echo
  echo "Cleaning up demo processes..."

  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" >/dev/null 2>&1; then
    kill "$API_PID" >/dev/null 2>&1 || true
    wait "$API_PID" >/dev/null 2>&1 || true
  fi

  if [[ -n "${WORKER_PID:-}" ]] && kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    kill "$WORKER_PID" >/dev/null 2>&1 || true
    wait "$WORKER_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

wait_for_url() {
  local url="$1"

  for _ in $(seq 1 30); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for $url"
  exit 1
}

queue_depth() {
  local status="$1"

  curl -s "$API_URL/metrics" \
    | awk -v status="$status" '
        $0 ~ "forgequeue_queue_depth\\{status=\"" status "\"\\}" {
          print $2
          exit
        }
      '
}

echo "ForgeQueue demo"
echo "==============="
echo

require_command docker
require_command curl
require_command make
require_command go

echo "Starting PostgreSQL..."
docker compose up -d

echo "Running migrations..."
make migrate

echo "Clearing jobs table for demo..."
docker exec -i forgequeue-postgres psql -U forgequeue -d forgequeue -c "TRUNCATE jobs;" >/dev/null

echo "Starting API on :8080..."
FORGEQUEUE_DATABASE_URL="$DB_URL" \
FORGEQUEUE_HTTP_ADDR=":8080" \
make run-api >/tmp/forgequeue-demo-api.log 2>&1 &

API_PID="$!"

wait_for_url "$API_URL/healthz"
echo "API is ready."

echo "Starting worker on :9090..."
FORGEQUEUE_DATABASE_URL="$DB_URL" \
FORGEQUEUE_WORKER_COUNT=5 \
FORGEQUEUE_WORKER_METRICS_ADDR=":9090" \
make run-worker >/tmp/forgequeue-demo-worker.log 2>&1 &

WORKER_PID="$!"

wait_for_url "http://localhost:9090/metrics"
echo "Worker metrics are ready."

echo
echo "Submitting $JOBS jobs..."

for i in $(seq 1 "$JOBS"); do
  curl -fsS -X POST "$API_URL/jobs" \
    -H "Content-Type: application/json" \
    -d "{\"kind\":\"test_job\",\"payload\":{\"demo\":true,\"index\":$i},\"max_retries\":3}" >/dev/null
done

echo "Jobs submitted."
echo

while true; do
  pending="$(queue_depth pending)"
  running="$(queue_depth running)"
  completed="$(queue_depth completed)"
  dead="$(queue_depth dead)"

  echo "pending=${pending:-0} running=${running:-0} completed=${completed:-0} dead=${dead:-0}"

  if [[ "${pending:-0}" == "0" && "${running:-0}" == "0" && "${completed:-0}" == "$JOBS" ]]; then
    break
  fi

  sleep 1
done

echo
echo "Demo finished successfully."
echo
echo "Useful checks:"
echo "curl -s $API_URL/metrics | grep forgequeue_queue_depth"
echo "curl -s http://localhost:9090/metrics | grep forgequeue_active_workers"
echo
echo "Logs:"
echo "/tmp/forgequeue-demo-api.log"
echo "/tmp/forgequeue-demo-worker.log"