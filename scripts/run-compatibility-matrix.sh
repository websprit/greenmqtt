#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/summary.sh"

MODE="smoke"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
summary_init_state

for arg in "$@"; do
  case "$arg" in
    --release) MODE="release" ;;
    *)
      echo "usage: $0 [--release]" >&2
      exit 2
      ;;
  esac
done

if [[ "$MODE" == "release" ]]; then
  summary_run_step "compat:${MODE}" "mqtt311-baseline" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "mqtt5-baseline" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_v5_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "tcp-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "tls-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_tls_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "ws-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::publish::mqtt_ws_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "wss-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::wss_tests::mqtt_wss_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "quic-flow" \
    cargo test --release -p greenmqtt-broker mqtt::tests::quic_tests::mqtt_quic_connect_subscribe_publish_flow -- --nocapture
else
  summary_run_step "compat:${MODE}" "mqtt311-baseline" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "mqtt5-baseline" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_v5_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "tcp-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "tls-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tls_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "ws-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_ws_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "wss-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::wss_tests::mqtt_wss_connect_subscribe_publish_flow -- --nocapture

  summary_run_step "compat:${MODE}" "quic-flow" \
    cargo test -p greenmqtt-broker mqtt::tests::quic_tests::mqtt_quic_connect_subscribe_publish_flow -- --nocapture
fi

summary_emit_results_profile "compatibility-${MODE}" "$SUMMARY_FILE"
exit "$OVERALL_STATUS"
