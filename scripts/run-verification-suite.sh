#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

MODE="${1:-full}"
RELEASE_MODE="default"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

if [[ "${MODE}" == "--release" ]]; then
  MODE="full"
  RELEASE_MODE="release"
elif [[ "${2:-}" == "--release" ]]; then
  RELEASE_MODE="release"
fi

case "$MODE" in
  full)
    if [[ "$RELEASE_MODE" == "release" ]]; then
      summary_run_step "verification:${MODE}" "performance-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-performance-profile.sh" --release
      summary_run_step "verification:${MODE}" "cluster-local-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-cluster-profile.sh" local --release
      summary_run_step "verification:${MODE}" "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    else
      summary_run_step "verification:${MODE}" "performance-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-performance-profile.sh"
      summary_run_step "verification:${MODE}" "cluster-local-profile" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-cluster-profile.sh" local
      summary_run_step "verification:${MODE}" "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    fi
    ;;
  quick)
    summary_run_step "verification:${MODE}" "workspace-smoke" env -u GREENMQTT_PROFILE_SUMMARY_FILE "$ROOT_DIR/scripts/run-workspace-smoke.sh"
    ;;
  *)
    echo "usage: $0 [full|quick] [--release]" >&2
    exit 2
    ;;
esac

summary_emit_results_profile "verification-${MODE}-${RELEASE_MODE}" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
