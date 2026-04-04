#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TODO_FILE="${ROOT_DIR%/greenmqtt}/TODO.md"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"

ruby -rjson -e '
  todo = File.read(ARGV[0])
  root = ARGV[1]

  unchecked = todo.scan(/^- \[ \]/).size
  checked = todo.scan(/^- \[x\]/i).size
  milestones = todo.scan(/^\| \*\*M\d+\*\*/).size

  scripts = Dir[File.join(root, "scripts", "**", "*.sh")].size
  docs = Dir[File.join(root, "docs", "*.md")].size

  payload = {
    profile: "freeze-summary",
    status: unchecked.zero? ? 0 : 1,
    todo_checked_items: checked,
    todo_unchecked_items: unchecked,
    milestone_count: milestones,
    script_count: scripts,
    doc_count: docs,
  }

  puts JSON.generate(payload)
' "$TODO_FILE" "$ROOT_DIR" | {
  if [[ -n "$SUMMARY_FILE" ]]; then
    tee "$SUMMARY_FILE"
  else
    cat
  fi
}
