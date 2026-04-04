#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-full}"
RELEASE_MODE="default"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
RESULTS=()

if [[ "${MODE}" == "--release" ]]; then
  MODE="full"
  RELEASE_MODE="release"
elif [[ "${2:-}" == "--release" ]]; then
  RELEASE_MODE="release"
fi

run_step() {
  local name="$1"
  shift

  echo "[verification:${MODE}] ${name}"
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
  local summary="{\"profile\":\"verification-${MODE}-${RELEASE_MODE}\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

case "$MODE" in
  full)
    if [[ "$RELEASE_MODE" == "release" ]]; then
      run_step "performance-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-performance-profile.sh" --release
      run_step "cluster-local-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-cluster-profile.sh" local --release
      run_step "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    else
      run_step "performance-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-performance-profile.sh"
      run_step "cluster-local-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-cluster-profile.sh" local
      run_step "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    fi
    ;;
  quick)
    run_step "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    ;;
  *)
    echo "usage: $0 [full|quick] [--release]" >&2
    exit 2
    ;;
esac

emit_summary
exit "$OVERALL_STATUS"
