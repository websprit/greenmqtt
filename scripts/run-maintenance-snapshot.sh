#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"

ruby -rjson -e '
  root = ARGV[0]

  docs = %w[
    archive-checklist.md
    ci-artifacts.md
    docs-map.md
    final-audit.md
    freeze-checklist.md
    gates.md
    handoff-index.md
    maintenance-index.md
    operations-index.md
    post-release-inspection.md
    rollback-drills.md
    verification-profiles.md
  ].map { |name| File.join(root, "docs", name) }

  scripts = %w[
    run-release-summary.sh
    run-nightly-matrix.sh
    run-post-release-inspection.sh
    run-final-acceptance.sh
    run-final-audit.sh
    run-final-inventory.sh
    run-freeze-summary.sh
    evaluate-gate.sh
    render-summary-notification.sh
    run-summary-diff.sh
    view-ci-artifacts.sh
  ].map { |name| File.join(root, "scripts", name) }

  payload = {
    profile: "maintenance-snapshot",
    status: 0,
    docs: docs,
    scripts: scripts,
    gate_and_summary_entries: scripts.select { |path|
      File.basename(path).include?("summary") ||
        File.basename(path).include?("gate") ||
        File.basename(path).include?("acceptance")
    },
  }

  puts JSON.generate(payload)
' "$ROOT_DIR" | {
  if [[ -n "$SUMMARY_FILE" ]]; then
    tee "$SUMMARY_FILE"
  else
    cat
  fi
}
