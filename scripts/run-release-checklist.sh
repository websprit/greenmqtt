#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="print"

for arg in "$@"; do
  case "$arg" in
    --execute) MODE="execute" ;;
    --json) MODE="json" ;;
    *)
      echo "usage: $0 [--execute|--json]" >&2
      exit 2
      ;;
  esac
done

STEPS=(
  "1. ./scripts/run-workspace-smoke.sh"
  "2. ./scripts/run-performance-profile.sh"
  "3. ./scripts/run-cluster-profile.sh local"
  "4. Verify no uncommitted release artifacts are present"
  "5. Capture summary JSON outputs for verification records"
)

if [[ "$MODE" == "json" ]]; then
  printf '{'
  printf '"profile":"release-checklist",'
  printf '"steps":['
  for i in "${!STEPS[@]}"; do
    [[ $i -gt 0 ]] && printf ','
    printf '"%s"' "${STEPS[$i]}"
  done
  printf ']}\n'
  exit 0
fi

if [[ "$MODE" == "execute" ]]; then
  echo "[release] workspace smoke"
  ./scripts/run-workspace-smoke.sh

  echo "[release] performance profile"
  ./scripts/run-performance-profile.sh

  echo "[release] cluster local profile"
  ./scripts/run-cluster-profile.sh local

  echo "[release] checklist completed"
  exit 0
fi

echo "GreenMQTT release checklist"
for step in "${STEPS[@]}"; do
  echo "$step"
done
