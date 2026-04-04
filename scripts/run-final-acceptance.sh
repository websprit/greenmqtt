#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
TMP_DIR="$(mktemp -d)"
OVERALL_STATUS=0
RESULTS=()

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_step() {
  local name="$1"
  shift
  echo "[final-acceptance] ${name}"
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
  local summary="{\"profile\":\"final-acceptance\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

metrics_file="${TMP_DIR}/post-release-metrics.prom"
cat > "$metrics_file" <<'EOF'
mqtt_shard_move_total 1
mqtt_shard_failover_total 1
mqtt_shard_anti_entropy_total 2
mqtt_shard_fencing_reject_total 0
EOF

run_step "release-summary" \
  env GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/release-summary.json" \
  "$ROOT_DIR/scripts/run-release-summary.sh"

run_step "release-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" release "${TMP_DIR}/release-summary.json"

run_step "nightly-matrix" \
  env GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/nightly-matrix.json" \
  "$ROOT_DIR/scripts/run-nightly-matrix.sh"

run_step "nightly-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" nightly "${TMP_DIR}/nightly-matrix.json"

run_step "post-release-inspection" \
  env GREENMQTT_POST_RELEASE_AUDIT_JSON='[]' \
      GREENMQTT_METRICS_FILE="$metrics_file" \
      GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/post-release-inspection.json" \
      "$ROOT_DIR/scripts/run-post-release-inspection.sh" daily

run_step "post-release-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" post-release "${TMP_DIR}/post-release-inspection.json"

emit_summary
exit "$OVERALL_STATUS"
