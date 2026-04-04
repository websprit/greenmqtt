#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

summary_run_step "rollback-drill" "shard-move-rollback" \
  cargo test -p greenmqtt-cli shard_cli_command_drives_http_shard_move -- --nocapture

summary_run_step "rollback-drill" "cluster-failover-rollback" \
  cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

summary_run_step "rollback-drill" "config-rollback" \
  "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

summary_emit_results_profile "rollback-drill" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
