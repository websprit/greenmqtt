# Maintenance Index

## Read-only entry points

- [run-final-inventory.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-inventory.sh)
- [run-freeze-summary.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-freeze-summary.sh)
- [run-final-audit.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-final-audit.sh)
- [view-ci-artifacts.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/view-ci-artifacts.sh)
- [run-summary-diff.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/run-summary-diff.sh)
- [render-summary-notification.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/render-summary-notification.sh)
- [evaluate-gate.sh](/Users/jizhenggang/Documents/mqtt/greenmqtt/scripts/evaluate-gate.sh)

## Do not modify casually

- generated or machine-consumed summary schema scripts
- gate and notification scripts
- final acceptance / final audit / final inventory entry points

Change these only when the downstream consumers are updated in the same round.
