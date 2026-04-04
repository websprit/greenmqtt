# Freeze Checklist

## Version entry points

- [run-release-summary.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-release-summary.sh)
- [run-nightly-matrix.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-nightly-matrix.sh)
- [run-final-acceptance.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-acceptance.sh)

## Verification entry points

- [run-verification-suite.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-verification-suite.sh)
- [run-performance-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-performance-profile.sh)
- [run-cluster-profile.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-cluster-profile.sh)
- [run-post-release-inspection.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-post-release-inspection.sh)
- [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)

## Documentation entry points

- [handoff-index.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/handoff-index.md)
- [operations-index.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/operations-index.md)
- [docs-map.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/docs-map.md)
- [archive-checklist.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/archive-checklist.md)

## Freeze conditions

- `TODO.md` has no unchecked items for the active delivery plan
- freeze summary is generated
- final acceptance has a green result
- final audit report exists, even if it flags a dirty worktree
