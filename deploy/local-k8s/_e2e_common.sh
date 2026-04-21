#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${GREENMQTT_NAMESPACE:-greenmqtt}"
RELEASE="${GREENMQTT_RELEASE:-greenmqtt}"
HTTP_HOST="${GREENMQTT_HTTP_HOST:-greenmqtt-0.greenmqtt-headless}"
HTTP_BASE="${GREENMQTT_HTTP_BASE:-http://${HTTP_HOST}:8080}"
MQTT_HOST="${GREENMQTT_MQTT_HOST:-greenmqtt}"
MQTT_PORT="${GREENMQTT_MQTT_PORT:-1883}"
E2E_TIMEOUT="${GREENMQTT_E2E_TIMEOUT:-180s}"
SUFFIX="${GREENMQTT_E2E_SUFFIX:-$(date +%s)-$RANDOM}"

wait_greenmqtt_ready() {
  kubectl -n "${NAMESPACE}" wait \
    --for=condition=ready \
    pod/greenmqtt-0 \
    pod/greenmqtt-1 \
    pod/greenmqtt-2 \
    pod/greenmqtt-state-0 \
    --timeout="${E2E_TIMEOUT}" \
    >/dev/null
}

extract_session_id() {
  printf '%s' "$1" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p'
}

require_contains() {
  local haystack="$1"
  local needle="$2"
  local message="${3:-expected response to contain ${needle}}"
  if ! printf '%s\n' "${haystack}" | grep -q "${needle}"; then
    printf '%s\n' "${message}" >&2
    printf '%s\n' "${haystack}" >&2
    return 1
  fi
}

run_curl_pod() {
  local label="$1"
  local script="$2"
  local pod_name="greenmqtt-${label}-${SUFFIX}"
  kubectl run -n "${NAMESPACE}" "${pod_name}" \
    --image=curlimages/curl:8.8.0 \
    --restart=Never \
    --rm -i \
    --command -- sh -ec "${script}"
}

run_mqtt_pod() {
  local label="$1"
  local script="$2"
  local pod_name="greenmqtt-${label}-${SUFFIX}"
  kubectl run -n "${NAMESPACE}" "${pod_name}" \
    --image=eclipse-mosquitto:2 \
    --restart=Never \
    --rm -i \
    --command -- sh -ec "${script}"
}
