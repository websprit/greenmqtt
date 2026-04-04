#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[smoke] clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "[smoke] workspace tests"
cargo test --workspace

echo "[smoke] cluster profile (ci)"
"$ROOT_DIR/scripts/run-cluster-profile.sh" ci

echo "[smoke] completed"
