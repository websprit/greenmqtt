#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${1:-local}"

case "$PROFILE" in
  local)
    echo "[cluster-profile:local] failover recovery"
    cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

    echo "[cluster-profile:local] repeated failover soak"
    GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-3}" \
      cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture

    echo "[cluster-profile:local] sessiondict handoff"
    cargo test -p greenmqtt-cli cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --nocapture

    if [[ "${GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK:-0}" == "1" ]]; then
      echo "[cluster-profile:local] optional flaky soak"
      cargo test -p greenmqtt-cli cluster_redis_soak_leaves_no_residual_state -- --ignored --nocapture
    fi
    ;;

  ci)
    echo "[cluster-profile:ci] reduced cross-node bench"
    GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS="${GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS:-4}" \
    GREENMQTT_CLUSTER_BENCH_MESSAGES="${GREENMQTT_CLUSTER_BENCH_MESSAGES:-8}" \
    GREENMQTT_CLUSTER_BENCH_QOS="${GREENMQTT_CLUSTER_BENCH_QOS:-1}" \
    GREENMQTT_CLUSTER_BENCH_SCENARIO="${GREENMQTT_CLUSTER_BENCH_SCENARIO:-fanout}" \
    GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC="${GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC:-10}" \
    GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES="${GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES:-268435456}" \
      cargo test -p greenmqtt-cli cluster_redis_cross_node_bench_meets_delivery_thresholds -- --nocapture

    echo "[cluster-profile:ci] reduced failover soak"
    GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS="${GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS:-2}" \
      cargo test -p greenmqtt-cli cluster_redis_repeated_failover_replays_without_offline_leak -- --nocapture
    ;;

  *)
    echo "usage: $0 [local|ci]" >&2
    exit 2
    ;;
esac

echo "[cluster-profile:$PROFILE] completed"
