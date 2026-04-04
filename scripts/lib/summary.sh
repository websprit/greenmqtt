#!/usr/bin/env bash

summary_init_state() {
  OVERALL_STATUS=0
  RESULTS=()
  COMPONENTS=()
}

summary_record_result() {
  local name="$1"
  local status="$2"
  local duration="$3"
  if [[ "$status" -ne 0 ]]; then
    OVERALL_STATUS=1
  fi
  RESULTS+=("{\"name\":\"${name}\",\"status\":${status},\"duration_seconds\":${duration}}")
}

summary_run_step() {
  local prefix="$1"
  local name="$2"
  shift 2
  echo "[${prefix}] ${name}"
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
  summary_record_result "$name" "$status" "$duration"
  return "$status"
}

summary_join_results() {
  local IFS=,
  printf '%s' "${RESULTS[*]}"
}

summary_join_components() {
  local IFS=,
  printf '%s' "${COMPONENTS[*]}"
}

summary_write() {
  local summary_file="${1:-}"
  local summary="$2"
  if [[ -n "$summary_file" ]]; then
    printf '%s\n' "$summary" > "$summary_file"
  fi
  printf '%s\n' "$summary"
}

summary_emit_results_profile() {
  local profile="$1"
  local summary_file="$2"
  local extra_json='{}'
  if [[ $# -ge 3 ]]; then
    extra_json="$3"
  fi
  local joined
  joined="$(summary_join_results)"
  local summary
  summary="$(ruby -rjson -e '
    profile = ARGV[0]
    extra = JSON.parse(ARGV[1])
    results = JSON.parse("[" + ARGV[2] + "]")
    payload = { profile: profile, status: Integer(ARGV[3]), results: results }
    payload.merge!(extra)
    puts JSON.generate(payload)
  ' "$profile" "$extra_json" "$joined" "$OVERALL_STATUS")"
  summary_write "$summary_file" "$summary"
}

summary_record_component() {
  local name="$1"
  local status="$2"
  local duration="$3"
  local nested_summary="$4"
  local required="${5:-required}"
  if [[ "$status" -ne 0 && "$required" == "required" ]]; then
    OVERALL_STATUS=1
  fi
  local component_json
  component_json="$(ruby -rjson -e '
    name = ARGV[0]
    status = Integer(ARGV[1])
    duration = Integer(ARGV[2])
    nested = JSON.parse(ARGV[3])
    required = ARGV[4]
    payload = {
      name: name,
      status: status,
      duration_seconds: duration,
      profile: nested["profile"],
      summary: nested,
    }
    payload[:required] = (required == "required") unless required == "omit"
    puts JSON.generate(payload)
  ' "$name" "$status" "$duration" "$nested_summary" "$required")"
  COMPONENTS+=("$component_json")
}

summary_run_component() {
  local prefix="$1"
  local tmp_dir="$2"
  local name="$3"
  local required="${4:-required}"
  shift 4

  local component_summary="${tmp_dir}/${name}.json"
  echo "[${prefix}] ${name}"
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

  local nested_summary='{"profile":"missing","status":1,"results":[]}'
  if [[ -f "$component_summary" ]]; then
    nested_summary="$(cat "$component_summary")"
  fi

  summary_record_component "$name" "$status" "$duration" "$nested_summary" "$required"
  return "$status"
}

summary_emit_components_profile() {
  local profile="$1"
  local summary_file="$2"
  local extra_json='{}'
  if [[ $# -ge 3 ]]; then
    extra_json="$3"
  fi
  local joined
  joined="$(summary_join_components)"
  local summary
  summary="$(ruby -rjson -rtime -e '
    profile = ARGV[0]
    extra = JSON.parse(ARGV[1])
    components = JSON.parse("[" + ARGV[2] + "]")
    payload = {
      profile: profile,
      status: Integer(ARGV[3]),
      generated_at: Time.now.utc.iso8601,
      components: components,
    }
    payload.merge!(extra)
    puts JSON.generate(payload)
  ' "$profile" "$extra_json" "$joined" "$OVERALL_STATUS")"
  summary_write "$summary_file" "$summary"
}
