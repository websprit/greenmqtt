#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
EXECUTE_MODE=0
summary_init_state

if [[ -n "${GREENMQTT_CLI_BIN:-}" ]]; then
  CLI=("${GREENMQTT_CLI_BIN}" shard)
else
  CLI=(cargo run -q -p greenmqtt-cli -- shard)
fi

usage() {
  echo "usage: $0 [rebalance|failover] <kind> <tenant_id> <scope> <target_node_id> [--execute]" >&2
  exit 2
}

emit_summary() {
  local mode="dry-run"
  if [[ $EXECUTE_MODE -eq 1 ]]; then
    mode="execute"
  fi
  summary_emit_results_profile "shard-recovery" "$SUMMARY_FILE" "{\"mode\":\"${mode}\"}"
}

subcommand="${1:-}"
kind="${2:-}"
tenant_id="${3:-}"
scope="${4:-}"
target_node_id="${5:-}"

[[ -n "$subcommand" && -n "$kind" && -n "$tenant_id" && -n "$scope" && -n "$target_node_id" ]] || usage

shift 5
for arg in "$@"; do
  case "$arg" in
    --execute) EXECUTE_MODE=1 ;;
    *) usage ;;
  esac
done

case "$subcommand" in
  rebalance)
    if [[ $EXECUTE_MODE -eq 1 ]]; then
      summary_run_step "shard-recovery" "drain" "${CLI[@]}" drain "$kind" "$tenant_id" "$scope"
      summary_run_step "shard-recovery" "catch-up" "${CLI[@]}" catch-up "$kind" "$tenant_id" "$scope" "$target_node_id"
      summary_run_step "shard-recovery" "move" "${CLI[@]}" move "$kind" "$tenant_id" "$scope" "$target_node_id"
    else
      summary_run_step "shard-recovery" "drain" "${CLI[@]}" drain "$kind" "$tenant_id" "$scope" --dry-run
      summary_run_step "shard-recovery" "catch-up" "${CLI[@]}" catch-up "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
      summary_run_step "shard-recovery" "move" "${CLI[@]}" move "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
    fi
    ;;
  failover)
    if [[ $EXECUTE_MODE -eq 1 ]]; then
      summary_run_step "shard-recovery" "failover" "${CLI[@]}" failover "$kind" "$tenant_id" "$scope" "$target_node_id"
      summary_run_step "shard-recovery" "repair" "${CLI[@]}" repair "$kind" "$tenant_id" "$scope" "$target_node_id"
    else
      summary_run_step "shard-recovery" "failover" "${CLI[@]}" failover "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
      summary_run_step "shard-recovery" "repair" "${CLI[@]}" repair "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
    fi
    ;;
  *)
    usage
    ;;
esac

emit_summary
exit "$OVERALL_STATUS"
