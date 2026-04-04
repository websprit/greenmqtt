#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"

ruby -rjson -e '
  root = ARGV[0]
  scripts = %w[
    run-release-summary.sh
    run-verification-suite.sh
    run-performance-profile.sh
    run-cluster-profile.sh
    run-compatibility-matrix.sh
    run-storage-upgrade-baseline.sh
    run-rollback-drill.sh
    run-shard-recovery.sh
    run-config-rollback.sh
    run-post-release-inspection.sh
    run-scheduled-checks.sh
    run-nightly-matrix.sh
    run-summary-diff.sh
    render-summary-notification.sh
    evaluate-gate.sh
    view-ci-artifacts.sh
    run-final-acceptance.sh
  ]

  docs = %w[
    verification-profiles.md
    rollback-drills.md
    post-release-inspection.md
    ci-artifacts.md
    gates.md
    handoff-index.md
    operations-index.md
    docs-map.md
    archive-checklist.md
    shard-operations.md
  ]

  payload = {
    profile: "final-inventory",
    status: 0,
    scripts: scripts.map { |name| File.join(root, "scripts", name) },
    docs: docs.map { |name| File.join(root, "docs", name) },
    gate_and_summary_entries: [
      File.join(root, "scripts", "run-release-summary.sh"),
      File.join(root, "scripts", "run-nightly-matrix.sh"),
      File.join(root, "scripts", "run-post-release-inspection.sh"),
      File.join(root, "scripts", "evaluate-gate.sh"),
      File.join(root, "scripts", "render-summary-notification.sh"),
      File.join(root, "scripts", "run-summary-diff.sh"),
    ],
  }

  puts JSON.generate(payload)
' "$ROOT_DIR" | {
  if [[ -n "$SUMMARY_FILE" ]]; then
    tee "$SUMMARY_FILE"
  else
    cat
  fi
}
