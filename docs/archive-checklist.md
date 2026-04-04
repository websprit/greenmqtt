# Archive Checklist

## Code entry points

- [greenmqtt-cli/src/main.rs](/Users/jizhenggang/Documents/mqtt/greenmqtt/crates/greenmqtt-cli/src/main.rs)
- [greenmqtt-broker/src/lib.rs](/Users/jizhenggang/Documents/mqtt/greenmqtt/crates/greenmqtt-broker/src/lib.rs)
- [greenmqtt-http-api/src/lib.rs](/Users/jizhenggang/Documents/mqtt/greenmqtt/crates/greenmqtt-http-api/src/lib.rs)
- [greenmqtt-rpc/src/lib.rs](/Users/jizhenggang/Documents/mqtt/greenmqtt/crates/greenmqtt-rpc/src/lib.rs)

## Verification entry points

- [run-release-summary.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-release-summary.sh)
- [run-nightly-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-nightly-matrix.sh)
- [run-post-release-inspection.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-post-release-inspection.sh)
- [run-final-acceptance.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-acceptance.sh)

## Delivery and recovery entry points

- [run-rollback-drill.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-rollback-drill.sh)
- [run-shard-recovery.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-shard-recovery.sh)
- [run-config-rollback.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-config-rollback.sh)

## Result consumption entry points

- [run-summary-diff.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-summary-diff.sh)
- [render-summary-notification.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/render-summary-notification.sh)
- [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)
- [view-ci-artifacts.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/view-ci-artifacts.sh)

## Before archiving

- Confirm `TODO.md` has no unchecked items for the active delivery round.
- Confirm the final acceptance run is green.
- Confirm docs index and docs map include every operator-facing entry point.
- Confirm CI workflow and local scripts agree on artifact names.
