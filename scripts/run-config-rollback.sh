#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
EXECUTE_MODE=0
summary_init_state

usage() {
  echo "usage: $0 <storage-backend> [--execute]" >&2
  exit 2
}

emit_summary() {
  local mode="dry-run"
  if [[ $EXECUTE_MODE -eq 1 ]]; then
    mode="execute"
  fi
  summary_emit_results_profile "config-rollback" "$SUMMARY_FILE" "{\"mode\":\"${mode}\",\"target_backend\":\"${TARGET_BACKEND}\"}"
}

TARGET_BACKEND="${1:-}"
[[ -n "$TARGET_BACKEND" ]] || usage
shift

for arg in "$@"; do
  case "$arg" in
    --execute) EXECUTE_MODE=1 ;;
    *) usage ;;
  esac
done

case "$TARGET_BACKEND" in
  memory|sled|rocksdb|redis) ;;
  *) echo "unsupported storage backend: $TARGET_BACKEND" >&2; exit 2 ;;
esac

if [[ $EXECUTE_MODE -eq 1 ]]; then
  summary_run_step "config-rollback" "storage-upgrade-baseline" \
    env GREENMQTT_STORAGE_BACKEND="$TARGET_BACKEND" \
    "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

  summary_run_step "config-rollback" "verification-quick" \
    env GREENMQTT_STORAGE_BACKEND="$TARGET_BACKEND" \
    "$ROOT_DIR/scripts/run-verification-suite.sh" quick
else
  summary_run_step "config-rollback" "plan-storage-backend-export" \
    bash -lc "printf 'export GREENMQTT_STORAGE_BACKEND=%s\n' '$TARGET_BACKEND' >/dev/null"

  summary_run_step "config-rollback" "plan-storage-upgrade-baseline" \
    bash -lc "printf '%s\n' './scripts/run-storage-upgrade-baseline.sh' >/dev/null"

  summary_run_step "config-rollback" "plan-verification-quick" \
    bash -lc "printf '%s\n' './scripts/run-verification-suite.sh quick' >/dev/null"
fi

emit_summary
exit "$OVERALL_STATUS"
