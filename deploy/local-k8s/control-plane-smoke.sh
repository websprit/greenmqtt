#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NAMESPACE="${GREENMQTT_NAMESPACE:-greenmqtt}"
RELEASE="${GREENMQTT_RELEASE:-greenmqtt}"
RANGE_ID="${GREENMQTT_SMOKE_RANGE_ID:-control-smoke}"
STORE_NODE_ID="${GREENMQTT_STORE_NODE_ID:-1001}"
RPC_PORT="${GREENMQTT_STORE_RPC_LOCAL_PORT:-50051}"

cleanup() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "waiting for broker and store pods..."
kubectl -n "${NAMESPACE}" wait --for=condition=ready pod -l app.kubernetes.io/instance="${RELEASE}" --timeout=180s

echo "port-forwarding store rpc..."
kubectl -n "${NAMESPACE}" port-forward svc/${RELEASE}-state "${RPC_PORT}:50051" >/tmp/greenmqtt-control-plane-smoke.log 2>&1 &
PF_PID=$!
sleep 3

RANGE_ENDPOINT="http://127.0.0.1:${RPC_PORT}"
cd "${ROOT_DIR}"

echo "bootstrapping range ${RANGE_ID}..."
GREENMQTT_RANGE_CONTROL_ENDPOINT="${RANGE_ENDPOINT}" cargo run -p greenmqtt-cli -- range bootstrap \
  --range-id "${RANGE_ID}" \
  --kind retain \
  --tenant-id smoke \
  --scope '*' \
  --voters "${STORE_NODE_ID}" \
  --output text \
  >/tmp/greenmqtt-range-bootstrap.txt

echo "draining range ${RANGE_ID}..."
GREENMQTT_RANGE_CONTROL_ENDPOINT="${RANGE_ENDPOINT}" cargo run -p greenmqtt-cli -- range drain "${RANGE_ID}" --output text \
  >/tmp/greenmqtt-range-drain.txt

echo "retiring range ${RANGE_ID}..."
GREENMQTT_RANGE_CONTROL_ENDPOINT="${RANGE_ENDPOINT}" cargo run -p greenmqtt-cli -- range retire "${RANGE_ID}" --output text \
  >/tmp/greenmqtt-range-retire.txt

echo "control-plane smoke completed"
