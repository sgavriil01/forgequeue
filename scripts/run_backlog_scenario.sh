#!/usr/bin/env bash
set -Eeuo pipefail

trap 'echo "Script failed at line $LINENO while running: $BASH_COMMAND" >&2' ERR

API_URL="${API_URL:-http://localhost:8080}"
DB_CONTAINER="${DB_CONTAINER:-forgequeue-postgres}"
DB_USER="${DB_USER:-forgequeue}"
DB_NAME="${DB_NAME:-forgequeue}"
DB_URL="${FORGEQUEUE_DATABASE_URL:-postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable}"

WORKERS="${WORKERS:-5}"
JOBS="${JOBS:-10000}"
VUS="${VUS:-50}"

RESULTS_DIR="load-results/backlog-$(date +%Y%m%d-%H%M%S)"
RAW_DIR="$RESULTS_DIR/raw"
mkdir -p "$RAW_DIR"

SUMMARY_FILE="$RESULTS_DIR/summary.md"
SAMPLES_FILE="$RESULTS_DIR/queue_depth_samples.csv"
WORKER_LOG="$RAW_DIR/worker.log"
K6_LOG="$RAW_DIR/k6.log"
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

  sudo lsof -tiTCP:9090 -sTCP:LISTEN | xargs -r kill -9 || true
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

queue_depth_value() {
  local status="$1"

  local metrics
  if ! metrics="$(curl -fsS "$API_URL/metrics" 2>/dev/null)"; then
    echo ""
    return 0
  fi

  local value
  value="$(
    echo "$metrics" \
      | awk -v status="$status" '
          $0 ~ "forgequeue_queue_depth\\{status=\"" status "\"\\}" {
            print $2
            exit
          }
        '
  )"

  echo "${value:-}"
}

wait_for_worker_metrics() {
  for i in {1..30}; do
    if curl -fsS "http://localhost:9090/metrics" >/dev/null 2>&1; then
      return 0
    fi

    sleep 1
  done

  echo "Worker metrics endpoint did not become ready on :9090" >&2
  return 1
}

extract_api_p95() {
  local file="$1"

  awk '
    /^[[:space:]]*http_req_duration[.]*:/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^p\(95\)=/) {
          gsub(/^p\(95\)=/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "$file"
}

extract_http_failed() {
  local file="$1"

  awk '
    /^[[:space:]]*http_req_failed[.]*:/ {
      print $2
      exit
    }
  ' "$file"
}

worker_queue_latency_avg() {
  local metrics_file="$1"

  local sum
  local count

  sum="$(awk '/^forgequeue_queue_latency_seconds_sum\{kind="test_job"\}/ { print $2; exit }' "$metrics_file")"
  count="$(awk '/^forgequeue_queue_latency_seconds_count\{kind="test_job"\}/ { print $2; exit }' "$metrics_file")"

  if [[ -z "${sum:-}" || -z "${count:-}" || "$count" == "0" ]]; then
    echo "n/a"
    return
  fi

  awk -v sum="$sum" -v count="$count" 'BEGIN { printf "%.3fs", sum / count }'
}

sample_queue_depth() {
  echo "timestamp,pending,running,completed,dead" > "$SAMPLES_FILE"

  while true; do
    local ts
    local pending
    local running
    local completed
    local dead

    ts="$(date +%s)"

    pending="$(queue_depth_value pending || true)"
    running="$(queue_depth_value running || true)"
    completed="$(queue_depth_value completed || true)"
    dead="$(queue_depth_value dead || true)"

    if [[ -z "${pending:-}" || -z "${running:-}" || -z "${completed:-}" || -z "${dead:-}" ]]; then
      echo "metrics temporarily unavailable; retrying..."
      sleep 2
      continue
    fi

    echo "$ts,$pending,$running,$completed,$dead" >> "$SAMPLES_FILE"
    echo "pending=$pending running=$running completed=$completed dead=$dead"

    if [[ "$pending" == "0" && "$running" == "0" && "$completed" == "$JOBS" ]]; then
      break
    fi

    sleep 2
  done
}

require_api
cleanup_existing_workers
clear_jobs

echo "Starting worker with $WORKERS workers..."
setsid env \
  FORGEQUEUE_DATABASE_URL="$DB_URL" \
  FORGEQUEUE_WORKER_COUNT="$WORKERS" \
  FORGEQUEUE_WORKER_METRICS_ADDR=":9090" \
  "$WORKER_BIN" > "$WORKER_LOG" 2>&1 &

WORKER_PID=$!

cleanup_worker() {
  if kill -0 "$WORKER_PID" >/dev/null 2>&1; then
    kill -TERM "-$WORKER_PID" >/dev/null 2>&1 || true
    sleep 1
    kill -KILL "-$WORKER_PID" >/dev/null 2>&1 || true
    wait "$WORKER_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup_worker EXIT

wait_for_worker_metrics

echo "Running backlog k6 test: $JOBS jobs, $VUS VUs..."

STARTED_AT="$(date +%s)"

make load-submit LOAD_JOBS="$JOBS" LOAD_VUS="$VUS" 2>&1 | tee "$K6_LOG"

echo "Sampling queue depth until drained..."
sample_queue_depth

ENDED_AT="$(date +%s)"
TOTAL_TIME=$((ENDED_AT - STARTED_AT))

curl -s "$API_URL/metrics" > "$RAW_DIR/api_metrics_after.txt"
curl -s "http://localhost:9090/metrics" > "$RAW_DIR/worker_metrics_after.txt"

API_P95="$(extract_api_p95 "$K6_LOG")"
HTTP_FAILED="$(extract_http_failed "$K6_LOG")"
AVG_QUEUE_LATENCY="$(worker_queue_latency_avg "$RAW_DIR/worker_metrics_after.txt")"

MAX_PENDING="$(awk -F, 'NR > 1 { if ($2 > max) max = $2 } END { print max + 0 }' "$SAMPLES_FILE")"
MAX_RUNNING="$(awk -F, 'NR > 1 { if ($3 > max) max = $3 } END { print max + 0 }' "$SAMPLES_FILE")"
FINAL_COMPLETED="$(queue_depth_value completed)"
FINAL_DEAD="$(queue_depth_value dead)"
FINAL_PENDING="$(queue_depth_value pending)"
FINAL_RUNNING="$(queue_depth_value running)"

THROUGHPUT="$(awk -v jobs="$JOBS" -v seconds="$TOTAL_TIME" 'BEGIN { printf "%.2f jobs/s", jobs / seconds }')"

cat > "$SUMMARY_FILE" <<EOF
# ForgeQueue Backlog Load Test

Date: $(date)

## Scenario

| Workers | Jobs Submitted | VUs |
|---:|---:|---:|
| $WORKERS | $JOBS | $VUS |

## Results

| API p95 | HTTP Failures | Total Drain Time | Worker Throughput | Avg Queue Latency | Max Pending | Max Running | Final Completed | Final Dead | Final Pending | Final Running |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ${API_P95:-n/a} | ${HTTP_FAILED:-n/a} | ${TOTAL_TIME}s | $THROUGHPUT | $AVG_QUEUE_LATENCY | $MAX_PENDING | $MAX_RUNNING | $FINAL_COMPLETED | $FINAL_DEAD | $FINAL_PENDING | $FINAL_RUNNING |

## Interpretation

This scenario submits $JOBS jobs faster than the worker pool can process them.

The expected behavior is:

- queue depth grows during submission,
- workers continue processing steadily,
- queue depth eventually shrinks to zero,
- all jobs complete,
- no jobs are lost or moved to the dead-letter state.

Raw queue-depth samples are saved in:

\`$SAMPLES_FILE\`
EOF

echo
echo "Backlog scenario complete."
echo "Summary file:"
echo "$SUMMARY_FILE"
echo
cat "$SUMMARY_FILE"
