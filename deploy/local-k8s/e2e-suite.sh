#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/e2e-persistent-session.sh"
"${SCRIPT_DIR}/e2e-retain.sh"
"${SCRIPT_DIR}/e2e-offline-inbox.sh"

echo "all local-k8s e2e checks passed"
