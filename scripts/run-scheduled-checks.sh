#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-}"
RUN_MODE="execute"

if [[ -z "$MODE" ]]; then
  echo "usage: $0 <hourly|daily|nightly> [--dry-run]" >&2
  exit 2
fi

if [[ "${2:-}" == "--dry-run" ]]; then
  RUN_MODE="dry-run"
fi

case "$MODE" in
  hourly|daily)
    if [[ "$RUN_MODE" == "dry-run" ]]; then
      exec "$ROOT_DIR/scripts/run-post-release-inspection.sh" "$MODE" --dry-run
    else
      exec "$ROOT_DIR/scripts/run-post-release-inspection.sh" "$MODE"
    fi
    ;;
  nightly)
    if [[ "$RUN_MODE" == "dry-run" ]]; then
      printf '%s\n' "./scripts/run-nightly-matrix.sh"
      exit 0
    else
      exec "$ROOT_DIR/scripts/run-nightly-matrix.sh"
    fi
    ;;
  *)
    echo "usage: $0 <hourly|daily|nightly> [--dry-run]" >&2
    exit 2
    ;;
esac
