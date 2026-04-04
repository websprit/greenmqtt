#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

MODE="default"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

for arg in "$@"; do
  case "$arg" in
    --release) MODE="release" ;;
    *)
      echo "usage: $0 [--release]" >&2
      exit 2
      ;;
  esac
done

run_step_with_retries() {
  local name="$1"
  local retries="$2"
  shift 2

  local attempt=1
  local succeeded=0
  while (( attempt <= retries )); do
    local started_at
    started_at="$(date +%s)"
    local status=0
    echo "[profile:${MODE}] ${name}#${attempt}"
    set +e
    "$@"
    status=$?
    set -e
    local finished_at
    finished_at="$(date +%s)"
    local duration=$((finished_at - started_at))
    summary_record_result "${name}#${attempt}" "$status" "$duration"
    if [[ $status -eq 0 ]]; then
      succeeded=1
      break
    fi
    attempt=$((attempt + 1))
  done
  if [[ $succeeded -eq 0 ]]; then
    OVERALL_STATUS=1
    return 1
  fi
  return 0
}

TEST_ARGS=()
if [[ "$MODE" == "release" ]]; then
  TEST_ARGS+=(--release)
fi

if [[ "$MODE" == "release" ]]; then
  run_step_with_retries "metrics-overhead" 3 \
    cargo test "${TEST_ARGS[@]}" -p greenmqtt-cli metrics_collection_overhead_stays_below_one_percent -- --ignored --nocapture

  run_step_with_retries "backpressure-overhead" 3 \
    cargo test "${TEST_ARGS[@]}" -p greenmqtt-cli backpressure_overhead_stays_below_one_percent_under_normal_load -- --ignored --nocapture
else
  summary_run_step "profile:${MODE}" "metrics-overhead" \
    cargo test "${TEST_ARGS[@]}" -p greenmqtt-cli metrics_collection_overhead_stays_below_one_percent -- --ignored --nocapture

  summary_run_step "profile:${MODE}" "backpressure-overhead" \
    cargo test "${TEST_ARGS[@]}" -p greenmqtt-cli backpressure_overhead_stays_below_one_percent_under_normal_load -- --ignored --nocapture
fi

summary_run_step "profile:${MODE}" "bandwidth-shaping-throughput" \
  cargo test "${TEST_ARGS[@]}" -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_outbound_bandwidth_limit_keeps_throughput_under_1_2kbps -- --ignored --nocapture

summary_emit_results_profile "performance-${MODE}" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
