#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

PROFILE="${1:-local}"
MODE="default"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

if [[ "${PROFILE}" == "--release" ]]; then
  PROFILE="local"
  MODE="release"
elif [[ "${2:-}" == "--release" ]]; then
  MODE="release"
fi

case "$PROFILE" in
  local)
    if [[ "$MODE" == "release" ]]; then
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "failover-recovery" \
        cargo test --release -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-3}" \
        cargo test --release -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "sessiondict-handoff" \
        cargo test --release -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture

      if [[ "${GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK:-0}" == "1" ]]; then
        summary_run_step "cluster-profile:${PROFILE}:${MODE}" "optional-flaky-soak" \
          cargo test --release -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
      fi
    else
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "failover-recovery" \
        cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-3}" \
        cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "sessiondict-handoff" \
        cargo test -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture

      if [[ "${GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK:-0}" == "1" ]]; then
        summary_run_step "cluster-profile:${PROFILE}:${MODE}" "optional-flaky-soak" \
          cargo test -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
      fi
    fi
    ;;

  nightly)
    if [[ "$MODE" == "release" ]]; then
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-failover-recovery" \
        cargo test --release -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-6}" \
        cargo test --release -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-sessiondict-handoff" \
        cargo test --release -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture
    else
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-failover-recovery" \
        cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-repeated-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-6}" \
        cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "extended-sessiondict-handoff" \
        cargo test -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture
    fi
    ;;

  flaky)
    if [[ "$MODE" == "release" ]]; then
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "optional-flaky-soak" \
        cargo test --release -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
    else
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "optional-flaky-soak" \
        cargo test -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
    fi
    ;;

  ci)
    if [[ "$MODE" == "release" ]]; then
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "cross-node-bench" \
        env \
        GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS="${GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS:-4}" \
        GREENMQTT_CLUSTER_BENCH_MESSAGES="${GREENMQTT_CLUSTER_BENCH_MESSAGES:-8}" \
        GREENMQTT_CLUSTER_BENCH_QOS="${GREENMQTT_CLUSTER_BENCH_QOS:-1}" \
        GREENMQTT_CLUSTER_BENCH_SCENARIO="${GREENMQTT_CLUSTER_BENCH_SCENARIO:-fanout}" \
        GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC="${GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC:-10}" \
        GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES="${GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES:-268435456}" \
        cargo test --release -p greenmqtt-cli cluster_redis_cross_node_bench_meets_delivery_thresholds -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "reduced-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-2}" \
        cargo test --release -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture
    else
      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "cross-node-bench" \
        env \
        GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS="${GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS:-4}" \
        GREENMQTT_CLUSTER_BENCH_MESSAGES="${GREENMQTT_CLUSTER_BENCH_MESSAGES:-8}" \
        GREENMQTT_CLUSTER_BENCH_QOS="${GREENMQTT_CLUSTER_BENCH_QOS:-1}" \
        GREENMQTT_CLUSTER_BENCH_SCENARIO="${GREENMQTT_CLUSTER_BENCH_SCENARIO:-fanout}" \
        GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC="${GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC:-10}" \
        GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES="${GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES:-268435456}" \
        cargo test -p greenmqtt-cli cluster_redis_cross_node_bench_meets_delivery_thresholds -- --nocapture

      summary_run_step "cluster-profile:${PROFILE}:${MODE}" "reduced-failover-soak" \
        env GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-2}" \
        cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture
    fi
    ;;

  *)
    echo "usage: $0 [local|nightly|flaky|ci] [--release]" >&2
    exit 2
    ;;
esac

summary_emit_results_profile "cluster-${PROFILE}-${MODE}" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
