#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

MODE="release"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
TMP_DIR="$(mktemp -d)"
summary_init_state

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "verification" omit \
  "$ROOT_DIR/scripts/run-verification-suite.sh" full --release

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "performance" omit \
  "$ROOT_DIR/scripts/run-performance-profile.sh" --release

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "cluster" omit \
  "$ROOT_DIR/scripts/run-cluster-profile.sh" ci --release

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "compatibility" omit \
  "$ROOT_DIR/scripts/run-compatibility-matrix.sh" --release

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "storage-upgrade" omit \
  "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

summary_run_component "release-summary:${MODE}" "$TMP_DIR" "rollback-drill" omit \
  "$ROOT_DIR/scripts/run-rollback-drill.sh"

summary_emit_components_profile "release-summary" "$SUMMARY_FILE" '{"mode":"release"}'
exit "$OVERALL_STATUS"
