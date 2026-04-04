#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
EXECUTE_MODE=0
OVERALL_STATUS=0
RESULTS=()

if [[ -n "${GREENMQTT_CLI_BIN:-}" ]]; then
  CLI=("${GREENMQTT_CLI_BIN}" shard)
else
  CLI=(cargo run -q -p greenmqtt-cli -- shard)
fi

usage() {
  echo "usage: $0 [rebalance|failover] <kind> <tenant_id> <scope> <target_node_id> [--execute]" >&2
  exit 2
}

run_step() {
  local name="$1"
  shift
  echo "[shard-recovery] ${name}"
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
  local mode="dry-run"
  if [[ $EXECUTE_MODE -eq 1 ]]; then
    mode="execute"
  fi
  local summary="{\"profile\":\"shard-recovery\",\"mode\":\"${mode}\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
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
      run_step "drain" "${CLI[@]}" drain "$kind" "$tenant_id" "$scope"
      run_step "catch-up" "${CLI[@]}" catch-up "$kind" "$tenant_id" "$scope" "$target_node_id"
      run_step "move" "${CLI[@]}" move "$kind" "$tenant_id" "$scope" "$target_node_id"
    else
      run_step "drain" "${CLI[@]}" drain "$kind" "$tenant_id" "$scope" --dry-run
      run_step "catch-up" "${CLI[@]}" catch-up "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
      run_step "move" "${CLI[@]}" move "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
    fi
    ;;
  failover)
    if [[ $EXECUTE_MODE -eq 1 ]]; then
      run_step "failover" "${CLI[@]}" failover "$kind" "$tenant_id" "$scope" "$target_node_id"
      run_step "repair" "${CLI[@]}" repair "$kind" "$tenant_id" "$scope" "$target_node_id"
    else
      run_step "failover" "${CLI[@]}" failover "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
      run_step "repair" "${CLI[@]}" repair "$kind" "$tenant_id" "$scope" "$target_node_id" --dry-run
    fi
    ;;
  *)
    usage
    ;;
esac

emit_summary
exit "$OVERALL_STATUS"
