#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${1:-local}"
MODE="default"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
RESULTS=()
TEST_ARGS=()

if [[ "${PROFILE}" == "--release" ]]; then
  PROFILE="local"
  MODE="release"
elif [[ "${2:-}" == "--release" ]]; then
  MODE="release"
fi

run_step() {
  local name="$1"
  shift

  echo "[cluster-profile:${PROFILE}:${MODE}] ${name}"
  local started_at
  started_at="$(date +%s)"
  local status=0
  set +e
  "$@"
  status=$?
  set -e
  local finished_at
  finished_at="$(date +%s)"
  local duration=$((finished_at - started_at))

  if [[ $status -ne 0 ]]; then
    OVERALL_STATUS=1
  fi

  RESULTS+=("{\"name\":\"${name}\",\"status\":${status},\"duration_seconds\":${duration}}")
}

emit_summary() {
  local summary
  local joined=""
  local IFS=,
  joined="${RESULTS[*]}"
  summary="{\"profile\":\"cluster-${PROFILE}-${MODE}\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

case "$PROFILE" in
  local)
    if [[ "$MODE" == "release" ]]; then
      run_step "failover-recovery" \
        cargo test --release -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      run_step "repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-3}" \
        cargo test --release -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      run_step "sessiondict-handoff" \
        cargo test --release -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture

      if [[ "${GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK:-0}" == "1" ]]; then
        run_step "optional-flaky-soak" \
          cargo test --release -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
      fi
    else
      run_step "failover-recovery" \
        cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      run_step "repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-3}" \
        cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      run_step "sessiondict-handoff" \
        cargo test -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture

      if [[ "${GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK:-0}" == "1" ]]; then
        run_step "optional-flaky-soak" \
          cargo test -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
      fi
    fi
    ;;

  ci)
    if [[ "$MODE" == "release" ]]; then
      run_step "cross-node-bench" \
        env \
        GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS="${GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS:-4}" \
        GREENMQTT_CLUSTER_BENCH_MESSAGES="${GREENMQTT_CLUSTER_BENCH_MESSAGES:-8}" \
        GREENMQTT_CLUSTER_BENCH_QOS="${GREENMQTT_CLUSTER_BENCH_QOS:-1}" \
        GREENMQTT_CLUSTER_BENCH_SCENARIO="${GREENMQTT_CLUSTER_BENCH_SCENARIO:-fanout}" \
        GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC="${GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC:-10}" \
        GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES="${GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES:-268435456}" \
        cargo test --release -p greenmqtt-cli cluster_redis_cross_node_bench_meets_delivery_thresholds -- --nocapture

      run_step "reduced-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-2}" \
        cargo test --release -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture
    else
      run_step "cross-node-bench" \
        env \
        GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS="${GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS:-4}" \
        GREENMQTT_CLUSTER_BENCH_MESSAGES="${GREENMQTT_CLUSTER_BENCH_MESSAGES:-8}" \
        GREENMQTT_CLUSTER_BENCH_QOS="${GREENMQTT_CLUSTER_BENCH_QOS:-1}" \
        GREENMQTT_CLUSTER_BENCH_SCENARIO="${GREENMQTT_CLUSTER_BENCH_SCENARIO:-fanout}" \
        GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC="${GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC:-10}" \
        GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES="${GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES:-268435456}" \
        cargo test -p greenmqtt-cli cluster_redis_cross_node_bench_meets_delivery_thresholds -- --nocapture

      run_step "reduced-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-2}" \
        cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture
    fi
    ;;

  *)
    echo "usage: $0 [local|ci] [--release]" >&2
    exit 2
    ;;
esac

emit_summary
exit "$OVERALL_STATUS"
