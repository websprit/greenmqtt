#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
EXECUTE_MODE=0
OVERALL_STATUS=0
RESULTS=()

usage() {
  echo "usage: $0 <storage-backend> [--execute]" >&2
  exit 2
}

run_step() {
  local name="$1"
  shift
  echo "[config-rollback] ${name}"
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
  local summary="{\"profile\":\"config-rollback\",\"mode\":\"${mode}\",\"status\":${OVERALL_STATUS},\"target_backend\":\"${TARGET_BACKEND}\",\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
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
  run_step "storage-upgrade-baseline" \
    env GREENMQTT_STORAGE_BACKEND="$TARGET_BACKEND" \
    "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

  run_step "verification-quick" \
    env GREENMQTT_STORAGE_BACKEND="$TARGET_BACKEND" \
    "$ROOT_DIR/scripts/run-verification-suite.sh" quick
else
  run_step "plan-storage-backend-export" \
    bash -lc "printf 'export GREENMQTT_STORAGE_BACKEND=%s\n' '$TARGET_BACKEND' >/dev/null"

  run_step "plan-storage-upgrade-baseline" \
    bash -lc "printf '%s\n' './scripts/run-storage-upgrade-baseline.sh' >/dev/null"

  run_step "plan-verification-quick" \
    bash -lc "printf '%s\n' './scripts/run-verification-suite.sh quick' >/dev/null"
fi

emit_summary
exit "$OVERALL_STATUS"
