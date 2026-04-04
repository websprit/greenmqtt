#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
RESULTS=()

run_step() {
  local name="$1"
  shift
  echo "[rollback-drill] ${name}"
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
  local joined=""
  local IFS=,
  joined="${RESULTS[*]}"
  local summary="{\"profile\":\"rollback-drill\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

run_step "shard-move-rollback" \
  cargo test -p greenmqtt-cli shard_cli_command_drives_http_shard_move -- --nocapture

run_step "cluster-failover-rollback" \
  cargo test -p greenmqtt-cli cluster_redis_failover_falls_back_to_offline_and_recovers_on_reconnect -- --nocapture

run_step "config-rollback" \
  "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

emit_summary
exit "$OVERALL_STATUS"
