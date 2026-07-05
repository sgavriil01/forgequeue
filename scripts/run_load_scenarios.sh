#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
DB_CONTAINER="${DB_CONTAINER:-forgequeue-postgres}"
DB_USER="${DB_USER:-forgequeue}"
DB_NAME="${DB_NAME:-forgequeue}"
DB_URL="${FORGEQUEUE_DATABASE_URL:-postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable}"

RESULTS_DIR="load-results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

SUMMARY_FILE="$RESULTS_DIR/summary.md"
RAW_DIR="$RESULTS_DIR/raw"
mkdir -p "$RAW_DIR"

BIN_DIR="$RESULTS_DIR/bin"
WORKER_BIN="$BIN_DIR/forgequeue-worker"
mkdir -p "$BIN_DIR"

echo "Building worker binary..."
go build -o "$WORKER_BIN" ./cmd/worker

cat > "$SUMMARY_FILE" <<EOF
# ForgeQueue Load Test Results

Date: $(date)

API URL: \`$API_URL\`

| Scenario | Workers | Jobs | API p95 | HTTP Failures | Drain Time | Worker Throughput | Avg Queue Latency | Final Completed | Final Dead | Final Pending | Final Running |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
EOF

require_api() {
  if ! curl -fsS "$API_URL/healthz" >/dev/null; then
    echo "API is not reachable at $API_URL"
    echo "Start it first:"
    echo "FORGEQUEUE_DATABASE_URL=\"$DB_URL\" FORGEQUEUE_HTTP_ADDR=\":8080\" make run-api"
    exit 1
  fi
}

cleanup_existing_workers() {
  echo "Cleaning up old worker processes..."

  pkill -f 'go run ./cmd/worker' 2>/dev/null || true
  pkill -f '/tmp/go-build.*/exe/worker' 2>/dev/null || true
  pkill -f 'forgequeue-worker' 2>/dev/null || true

  sleep 1
}

clear_jobs() {
  echo "Clearing jobs table..."
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "TRUNCATE jobs;" >/dev/null
}

queue_depth_value() {
  local status="$1"

  local value
  value="$(
    curl -s "$API_URL/metrics" \
      | awk -v status="$status" '
          $0 ~ "forgequeue_queue_depth\\{status=\"" status "\"\\}" {
            print $2
            exit
          }
        '
  )"

  echo "${value:-0}"
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

wait_until_drained() {
  while true; do
    local pending
    local running

    pending="$(queue_depth_value pending)"
    running="$(queue_depth_value running)"

    echo "pending=$pending running=$running" >&2

    if [[ "$pending" == "0" && "$running" == "0" ]]; then
      break
    fi

    sleep 1
  done
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

  sum="$(
    awk '
      /^forgequeue_queue_latency_seconds_sum\{kind="test_job"\}/ {
        print $2
        exit
      }
    ' "$metrics_file"
  )"

  count="$(
    awk '
      /^forgequeue_queue_latency_seconds_count\{kind="test_job"\}/ {
        print $2
        exit
      }
    ' "$metrics_file"
  )"

  if [[ -z "${sum:-}" || -z "${count:-}" || "$count" == "0" ]]; then
    echo "n/a"
    return
  fi

  awk -v sum="$sum" -v count="$count" 'BEGIN { printf "%.3fs", sum / count }'
}

run_scenario() {
  local name="$1"
  local workers="$2"
  local jobs="$3"
  local vus="${4:-20}"

  echo
  echo "========================================"
  echo "Scenario: $name"
  echo "Workers: $workers"
  echo "Jobs: $jobs"
  echo "========================================"

  cleanup_existing_workers
  clear_jobs

  local worker_log="$RAW_DIR/${name}_worker.log"
  local k6_log="$RAW_DIR/${name}_k6.log"
  local api_metrics_before="$RAW_DIR/${name}_api_metrics_before.txt"
  local api_metrics_after="$RAW_DIR/${name}_api_metrics_after.txt"
  local worker_metrics_after="$RAW_DIR/${name}_worker_metrics_after.txt"

  curl -s "$API_URL/metrics" > "$api_metrics_before"

  echo "Starting worker..."
  setsid env \
    FORGEQUEUE_DATABASE_URL="$DB_URL" \
    FORGEQUEUE_WORKER_COUNT="$workers" \
    FORGEQUEUE_WORKER_METRICS_ADDR=":9090" \
    "$WORKER_BIN" > "$worker_log" 2>&1 &

  local worker_pid=$!

  cleanup_worker() {
    if kill -0 "$worker_pid" >/dev/null 2>&1; then
      kill -TERM "-$worker_pid" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL "-$worker_pid" >/dev/null 2>&1 || true
      wait "$worker_pid" >/dev/null 2>&1 || true
    fi
  }

  trap cleanup_worker RETURN

  wait_for_worker_metrics

  echo "Running k6..."

  local scenario_started
  scenario_started="$(date +%s)"

  make load-submit LOAD_JOBS="$jobs" LOAD_VUS="$vus" 2>&1 | tee "$k6_log"

  echo "Waiting for queue to drain..."
  wait_until_drained

  local scenario_ended
  scenario_ended="$(date +%s)"

  local drain_time
  drain_time=$((scenario_ended - scenario_started))

  curl -s "$API_URL/metrics" > "$api_metrics_after"
  curl -s "http://localhost:9090/metrics" > "$worker_metrics_after"

  local completed
  local dead
  local pending
  local running
  local api_p95
  local http_failed
  local throughput
  local avg_queue_latency

  completed="$(queue_depth_value completed)"
  dead="$(queue_depth_value dead)"
  pending="$(queue_depth_value pending)"
  running="$(queue_depth_value running)"

  api_p95="$(extract_api_p95 "$k6_log")"
  http_failed="$(extract_http_failed "$k6_log")"

  throughput="$(
    awk -v jobs="$jobs" -v seconds="$drain_time" '
      BEGIN {
        if (seconds <= 0) print "n/a";
        else printf "%.2f jobs/s", jobs / seconds
      }
    '
  )"

  avg_queue_latency="$(worker_queue_latency_avg "$worker_metrics_after")"

  cat >> "$SUMMARY_FILE" <<EOF
| $name | $workers | $jobs | ${api_p95:-n/a} | ${http_failed:-n/a} | ${drain_time}s | $throughput | $avg_queue_latency | $completed | $dead | $pending | $running |
EOF

  echo "Scenario complete: $name"
  echo "Stopping worker..."

  cleanup_worker
  trap - RETURN

  sleep 1
}

require_api
cleanup_existing_workers

run_scenario "one_worker_100_jobs" 1 100 20
run_scenario "one_worker_1000_jobs" 1 1000 20
run_scenario "five_workers_1000_jobs" 5 1000 20
run_scenario "ten_workers_1000_jobs" 10 1000 20

cleanup_existing_workers

echo
echo "All scenarios complete."
echo "Summary:"
echo "$SUMMARY_FILE"
echo
cat "$SUMMARY_FILE"