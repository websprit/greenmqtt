#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[profile] metrics overhead gate"
cargo test -p greenmqtt-cli metrics_collection_overhead_stays_below_one_percent -- --ignored --nocapture

echo "[profile] backpressure overhead gate"
cargo test -p greenmqtt-cli backpressure_overhead_stays_below_one_percent_under_normal_load -- --ignored --nocapture

echo "[profile] bandwidth shaping throughput gate"
cargo test -p greenmqtt-broker mqtt::tests::publish::mqtt_tcp_outbound_bandwidth_limit_keeps_throughput_under_1_2kbps -- --ignored --nocapture

echo "[profile] performance gates completed"
