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

summary_run_component "nightly-matrix" "$TMP_DIR" "cluster-nightly" required \
  "$ROOT_DIR/scripts/run-cluster-profile.sh" nightly

summary_run_component "nightly-matrix" "$TMP_DIR" "cluster-flaky" advisory \
  "$ROOT_DIR/scripts/run-cluster-profile.sh" flaky

summary_run_component "nightly-matrix" "$TMP_DIR" "compatibility-release" required \
  "$ROOT_DIR/scripts/run-compatibility-matrix.sh" --release

summary_run_component "nightly-matrix" "$TMP_DIR" "storage-upgrade" required \
  "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

summary_run_component "nightly-matrix" "$TMP_DIR" "rollback-drill" required \
  "$ROOT_DIR/scripts/run-rollback-drill.sh"

summary_emit_components_profile "nightly-matrix" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
