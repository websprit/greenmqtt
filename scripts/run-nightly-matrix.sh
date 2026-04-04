#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
TMP_DIR="$(mktemp -d)"
COMPONENTS=()

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_component() {
  local name="$1"
  local required="${2:-required}"
  shift 2

  local component_summary="${TMP_DIR}/${name}.json"
  echo "[nightly-matrix] ${name}"
  local started_at
  started_at="$(date +%s)"
  local status=0
  set +e
  GREENMQTT_PROFILE_SUMMARY_FILE="$component_summary" "$@"
  status=$?
  set -e
  local finished_at
  finished_at="$(date +%s)"
  local duration=$((finished_at - started_at))

  if [[ $status -ne 0 && "$required" == "required" ]]; then
    OVERALL_STATUS=1
  fi

  local nested_summary='{"profile":"missing","status":1,"results":[]}'
  if [[ -f "$component_summary" ]]; then
    nested_summary="$(cat "$component_summary")"
  fi

  local component_json
  component_json="$(ruby -rjson -rtime -e '
    name = ARGV[0]
    status = Integer(ARGV[1])
    duration = Integer(ARGV[2])
    nested = JSON.parse(ARGV[3])
    required = ARGV[4]
    puts JSON.generate({
      name: name,
      status: status,
      required: required == "required",
      duration_seconds: duration,
      profile: nested["profile"],
      summary: nested,
    })
  ' "$name" "$status" "$duration" "$nested_summary" "$required")"
  COMPONENTS+=("$component_json")
}

emit_summary() {
  local joined=""
  local IFS=,
  joined="${COMPONENTS[*]}"
  local summary
  summary="$(ruby -rjson -rtime -e '
    generated_at = Time.now.utc.iso8601
    status = Integer(ARGV[0])
    components = JSON.parse("[" + ARGV[1] + "]")
    puts JSON.generate({
      profile: "nightly-matrix",
      status: status,
      generated_at: generated_at,
      components: components,
    })
  ' "$OVERALL_STATUS" "$joined")"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf "%s\n" "$summary" > "$SUMMARY_FILE"
  fi
  printf "%s\n" "$summary"
}

run_component "cluster-nightly" required \
  "$ROOT_DIR/scripts/run-cluster-profile.sh" nightly

run_component "cluster-flaky" advisory \
  "$ROOT_DIR/scripts/run-cluster-profile.sh" flaky

run_component "compatibility-release" required \
  "$ROOT_DIR/scripts/run-compatibility-matrix.sh" --release

run_component "storage-upgrade" required \
  "$ROOT_DIR/scripts/run-storage-upgrade-baseline.sh"

run_component "rollback-drill" required \
  "$ROOT_DIR/scripts/run-rollback-drill.sh"

emit_summary
exit "$OVERALL_STATUS"
