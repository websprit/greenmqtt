# Handoff Index

## Core verification entry points

- Release baseline:
  - [run-release-summary.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-release-summary.sh)
  - [run-verification-suite.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-verification-suite.sh)
  - [run-performance-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-performance-profile.sh)
  - [run-cluster-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-cluster-profile.sh)
  - [run-compatibility-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-compatibility-matrix.sh)

- Recovery and rollback:
  - [run-storage-upgrade-baseline.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-storage-upgrade-baseline.sh)
  - [run-rollback-drill.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-rollback-drill.sh)
  - [run-shard-recovery.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-shard-recovery.sh)
  - [run-config-rollback.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-config-rollback.sh)

- Inspection and scheduling:
  - [run-post-release-inspection.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-post-release-inspection.sh)
  - [run-scheduled-checks.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-scheduled-checks.sh)
  - [run-nightly-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-nightly-matrix.sh)

- Result consumption:
  - [run-summary-diff.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-summary-diff.sh)
  - [render-summary-notification.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/render-summary-notification.sh)
  - [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)
  - [view-ci-artifacts.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/view-ci-artifacts.sh)

## Supporting docs

- [verification-profiles.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/verification-profiles.md)
- [rollback-drills.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/rollback-drills.md)
- [post-release-inspection.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/post-release-inspection.md)
- [ci-artifacts.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/ci-artifacts.md)
- [gates.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/gates.md)
- [shard-operations.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/shard-operations.md)

## Recommended acceptance sequence

1. Run release summary.
2. Evaluate the release gate.
3. Run nightly matrix.
4. Evaluate the nightly gate.
5. Run post-release inspection in daily mode.
6. Evaluate the post-release gate.
7. If any gate blocks, use rollback or shard recovery flows before proceeding.
