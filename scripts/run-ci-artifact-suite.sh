#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ARTIFACT_DIR="${1:-$ROOT_DIR/.tmp/ci-artifacts}"
mkdir -p "$ARTIFACT_DIR"

metrics_file="$ARTIFACT_DIR/post-release-metrics.prom"
cat > "$metrics_file" <<'EOF'
mqtt_shard_move_total 1
mqtt_shard_failover_total{tenant_id="t1"} 1
mqtt_shard_anti_entropy_total 2
mqtt_shard_fencing_reject_total{kind="dist"} 0
EOF

echo "[ci-artifact-suite] workspace smoke"
./scripts/run-workspace-smoke.sh

echo "[ci-artifact-suite] release summary"
GREENMQTT_PROFILE_SUMMARY_FILE="$ARTIFACT_DIR/release-summary.json" \
  ./scripts/run-release-summary.sh

echo "[ci-artifact-suite] rollback drill"
GREENMQTT_PROFILE_SUMMARY_FILE="$ARTIFACT_DIR/rollback-drill.json" \
  ./scripts/run-rollback-drill.sh

echo "[ci-artifact-suite] post-release inspection"
GREENMQTT_PROFILE_SUMMARY_FILE="$ARTIFACT_DIR/post-release-inspection.json" \
GREENMQTT_POST_RELEASE_AUDIT_JSON='[]' \
GREENMQTT_METRICS_FILE="$metrics_file" \
  ./scripts/run-post-release-inspection.sh

echo "[ci-artifact-suite] produced artifacts"
ls -1 "$ARTIFACT_DIR"
