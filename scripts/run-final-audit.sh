#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TODO_FILE="${ROOT_DIR%/greenmqtt}/TODO.md"
SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"

key_entries=(
  "$ROOT_DIR/scripts/run-release-summary.sh"
  "$ROOT_DIR/scripts/run-nightly-matrix.sh"
  "$ROOT_DIR/scripts/run-post-release-inspection.sh"
  "$ROOT_DIR/scripts/run-final-acceptance.sh"
  "$ROOT_DIR/scripts/evaluate-gate.sh"
  "$ROOT_DIR/docs/handoff-index.md"
  "$ROOT_DIR/docs/operations-index.md"
  "$ROOT_DIR/docs/archive-checklist.md"
)

missing_entries=()
for entry in "${key_entries[@]}"; do
  [[ -e "$entry" ]] || missing_entries+=("$entry")
done

unchecked_count="$(rg -n '^- \[ \]' "$TODO_FILE" | wc -l | tr -d ' ')"
git_status="$(git -C "$ROOT_DIR" status --short)"

status=0
[[ "$unchecked_count" == "0" ]] || status=1
[[ -z "$git_status" ]] || status=1
[[ "${#missing_entries[@]}" == "0" ]] || status=1

missing_json='[]'
if [[ "${#missing_entries[@]}" -gt 0 ]]; then
  missing_json="$(ruby -rjson -e 'puts JSON.generate(ARGV)' "${missing_entries[@]}")"
fi

ruby -rjson -e '
  payload = {
    profile: "final-audit",
    status: Integer(ARGV[0]),
    todo_unchecked_items: Integer(ARGV[1]),
    worktree_clean: ARGV[2].empty?,
    git_status: ARGV[2].lines.map(&:chomp),
    missing_entries: JSON.parse(ARGV[3]),
  }
  puts JSON.generate(payload)
' "$status" "$unchecked_count" "$git_status" "$missing_json" | {
  if [[ -n "$SUMMARY_FILE" ]]; then
    tee "$SUMMARY_FILE"
  else
    cat
  fi
}

exit "$status"
