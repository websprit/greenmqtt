#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

summary_run_step "storage-upgrade" "memory-to-durable" \
  cargo test -p greenmqtt-storage memory_to_ -- --nocapture

summary_run_step "storage-upgrade" "durable-switch" \
  cargo test -p greenmqtt-storage _switch_preserves_session_route_retain_state -- --nocapture

summary_emit_results_profile "storage-upgrade-baseline" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
