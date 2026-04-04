#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="smoke"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
OVERALL_STATUS=0
RESULTS=()

for arg in "$@"; do
  case "$arg" in
    --release) MODE="release" ;;
    *)
      echo "usage: $0 [--release]" >&2
      exit 2
      ;;
  esac
done

run_step() {
  local name="$1"
  shift
  echo "[compat:${MODE}] ${name}"
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
  local summary="{\"profile\":\"compatibility-${MODE}\",\"status\":${OVERALL_STATUS},\"results\":[${joined}]}"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

if [[ "$MODE" == "release" ]]; then
  run_step "mqtt311-baseline" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  run_step "mqtt5-baseline" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_v5_connect_subscribe_publish_flow -- --nocapture

  run_step "tcp-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  run_step "tls-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tls_connect_subscribe_publish_flow -- --nocapture

  run_step "ws-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_ws_connect_subscribe_publish_flow -- --nocapture

  run_step "wss-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::wss_tests::mqtt_wss_connect_subscribe_publish_flow -- --nocapture

  run_step "quic-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::quic_tests::mqtt_quic_connect_subscribe_publish_flow -- --nocapture
else
  run_step "mqtt311-baseline" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  run_step "mqtt5-baseline" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_v5_connect_subscribe_publish_flow -- --nocapture

  run_step "tcp-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  run_step "tls-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tls_connect_subscribe_publish_flow -- --nocapture

  run_step "ws-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_ws_connect_subscribe_publish_flow -- --nocapture

  run_step "wss-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::wss_tests::mqtt_wss_connect_subscribe_publish_flow -- --nocapture

  run_step "quic-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::quic_tests::mqtt_quic_connect_subscribe_publish_flow -- --nocapture
fi

emit_summary
exit "$OVERALL_STATUS"
