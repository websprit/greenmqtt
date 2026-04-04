#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
TMP_DIR="$(mktemp -d)"
summary_init_state

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

metrics_file="${TMP_DIR}/post-release-metrics.prom"
cat > "$metrics_file" <<'EOF'
mqtt_shard_move_total 1
mqtt_shard_failover_total 1
mqtt_shard_anti_entropy_total 2
mqtt_shard_fencing_reject_total 0
EOF

summary_run_step "final-acceptance" "release-summary" \
  env GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/release-summary.json" \
  "$ROOT_DIR/scripts/run-release-summary.sh"

summary_run_step "final-acceptance" "release-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" release "${TMP_DIR}/release-summary.json"

summary_run_step "final-acceptance" "nightly-matrix" \
  env GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/nightly-matrix.json" \
  "$ROOT_DIR/scripts/run-nightly-matrix.sh"

summary_run_step "final-acceptance" "nightly-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" nightly "${TMP_DIR}/nightly-matrix.json"

summary_run_step "final-acceptance" "post-release-inspection" \
  env GREENMQTT_POST_RELEASE_AUDIT_JSON='[]' \
      GREENMQTT_METRICS_FILE="$metrics_file" \
      GREENMQTT_PROFILE_SUMMARY_FILE="${TMP_DIR}/post-release-inspection.json" \
      "$ROOT_DIR/scripts/run-post-release-inspection.sh" daily

summary_run_step "final-acceptance" "post-release-gate" \
  "$ROOT_DIR/scripts/evaluate-gate.sh" post-release "${TMP_DIR}/post-release-inspection.json"

summary_emit_results_profile "final-acceptance" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
