#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
TMP_DIR="$(mktemp -d)"
MODE="${1:-daily}"
EXECUTE_MODE=1
summary_init_state

if [[ -n "${GREENMQTT_CLI_BIN:-}" ]]; then
  CLI=("${GREENMQTT_CLI_BIN}" shard)
else
  CLI=(cargo run -q -p greenmqtt-cli -- shard)
fi

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ "$MODE" == "--dry-run" ]]; then
  MODE="daily"
  EXECUTE_MODE=0
elif [[ "${2:-}" == "--dry-run" ]]; then
  EXECUTE_MODE=0
fi

emit_summary() {
  local run_mode="execute"
  if [[ $EXECUTE_MODE -eq 0 ]]; then
    run_mode="dry-run"
  fi
  summary_emit_results_profile "post-release-inspection" "$SUMMARY_FILE" "{\"mode\":\"${MODE}\",\"run_mode\":\"${run_mode}\"}"
}

case "$MODE" in
  hourly|daily) ;;
  *) echo "usage: $0 [hourly|daily] [--dry-run]" >&2; exit 2 ;;
esac

if [[ $EXECUTE_MODE -eq 0 ]]; then
  summary_run_step "post-release" "plan-shard-audit-window" \
    bash -lc "printf '%s\n' 'greenmqtt shard audit --limit ${GREENMQTT_SHARD_AUDIT_LIMIT:-20} --output json' >/dev/null"
  summary_run_step "post-release" "plan-shard-metrics-snapshot" \
    bash -lc "printf '%s\n' './scripts/run-shard-metrics-snapshot.sh' >/dev/null"
  if [[ "$MODE" == "daily" ]]; then
    summary_run_step "post-release" "plan-cluster-residual-check" \
      bash -lc "printf '%s\n' './scripts/run-cluster-profile.sh ci' >/dev/null"
  fi
else
  if [[ -n "${GREENMQTT_POST_RELEASE_AUDIT_JSON:-}" ]]; then
    audit_file="${TMP_DIR}/audit.json"
    printf '%s\n' "${GREENMQTT_POST_RELEASE_AUDIT_JSON}" > "$audit_file"
    summary_run_step "post-release" "shard-audit-window" cat "$audit_file"
  else
    summary_run_step "post-release" "shard-audit-window" \
      "${CLI[@]}" audit --limit "${GREENMQTT_SHARD_AUDIT_LIMIT:-20}" --output json
  fi

  summary_run_step "post-release" "shard-metrics-snapshot" \
    "$ROOT_DIR/scripts/run-shard-metrics-snapshot.sh"

  if [[ "$MODE" == "daily" ]]; then
    summary_run_step "post-release" "cluster-residual-check" \
      "$ROOT_DIR/scripts/run-cluster-profile.sh" ci
  fi
fi

emit_summary
exit "$OVERALL_STATUS"
