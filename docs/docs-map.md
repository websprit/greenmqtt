# Docs Map

## Operators / on-call

- [post-release-inspection.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/post-release-inspection.md)
  - script: [run-post-release-inspection.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-post-release-inspection.sh)
- [rollback-drills.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/rollback-drills.md)
  - scripts:
    - [run-rollback-drill.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-rollback-drill.sh)
    - [run-shard-recovery.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-shard-recovery.sh)
    - [run-config-rollback.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-config-rollback.sh)
- [shard-operations.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/shard-operations.md)
  - CLI: [greenmqtt-cli/src/main.rs](/Users/jizhenggang/Documents/mqtt/greenmqtt/crates/greenmqtt-cli/src/main.rs)

## Release owner

- [verification-profiles.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/verification-profiles.md)
  - scripts:
    - [run-verification-suite.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-verification-suite.sh)
    - [run-performance-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-performance-profile.sh)
    - [run-cluster-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-cluster-profile.sh)
    - [run-compatibility-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-compatibility-matrix.sh)
    - [run-storage-upgrade-baseline.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-storage-upgrade-baseline.sh)
- [handoff-index.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/handoff-index.md)
  - script: [run-final-acceptance.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-acceptance.sh)

## CI / automation maintainer

- [ci-artifacts.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/ci-artifacts.md)
  - scripts:
    - [run-ci-artifact-suite.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-ci-artifact-suite.sh)
    - [view-ci-artifacts.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/view-ci-artifacts.sh)
    - [run-summary-diff.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-summary-diff.sh)
- [gates.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/gates.md)
  - scripts:
    - [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)
    - [render-summary-notification.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/render-summary-notification.sh)
    - [run-scheduled-checks.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-scheduled-checks.sh)
    - [run-nightly-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-nightly-matrix.sh)

## Fast lookup

- Need to know whether to block a rollout:
  - [gates.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/gates.md)
- Need to compare two runs:
  - [run-summary-diff.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-summary-diff.sh)
- Need to inspect CI artifacts:
  - [view-ci-artifacts.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/view-ci-artifacts.sh)
- Need one end-to-end acceptance entry point:
  - [run-final-acceptance.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-acceptance.sh)
