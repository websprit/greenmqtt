#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
RESULTS=()
TMP_DIR="$(mktemp -d)"
MODE="${1:-daily}"
EXECUTE_MODE=1

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

run_step() {
  local name="$1"
  shift
  echo "[post-release] ${name}"
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
  local run_mode="execute"
  if [[ $EXECUTE_MODE -eq 0 ]]; then
    run_mode="dry-run"
  fi
  local summary="{\"profile\":\"post-release-inspection\",\"mode\":\"${MODE}\",\"run_mode\":\"${run_mode}\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

case "$MODE" in
  hourly|daily) ;;
  *) echo "usage: $0 [hourly|daily] [--dry-run]" >&2; exit 2 ;;
esac

if [[ $EXECUTE_MODE -eq 0 ]]; then
  run_step "plan-shard-audit-window" \
    bash -lc "printf '%s\n' 'greenmqtt shard audit --limit ${GREENMQTT_SHARD_AUDIT_LIMIT:-20} --output json' >/dev/null"
  run_step "plan-shard-metrics-snapshot" \
    bash -lc "printf '%s\n' './scripts/run-shard-metrics-snapshot.sh' >/dev/null"
  if [[ "$MODE" == "daily" ]]; then
    run_step "plan-cluster-residual-check" \
      bash -lc "printf '%s\n' './scripts/run-cluster-profile.sh ci' >/dev/null"
  fi
else
  if [[ -n "${GREENMQTT_POST_RELEASE_AUDIT_JSON:-}" ]]; then
    audit_file="${TMP_DIR}/audit.json"
    printf '%s\n' "${GREENMQTT_POST_RELEASE_AUDIT_JSON}" > "$audit_file"
    run_step "shard-audit-window" cat "$audit_file"
  else
    run_step "shard-audit-window" \
      "${CLI[@]}" audit --limit "${GREENMQTT_SHARD_AUDIT_LIMIT:-20}" --output json
  fi

  run_step "shard-metrics-snapshot" \
    "$ROOT_DIR/scripts/run-shard-metrics-snapshot.sh"

  if [[ "$MODE" == "daily" ]]; then
    run_step "cluster-residual-check" \
      "$ROOT_DIR/scripts/run-cluster-profile.sh" ci
  fi
fi

emit_summary
exit "$OVERALL_STATUS"
