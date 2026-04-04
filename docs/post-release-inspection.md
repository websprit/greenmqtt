# Post-release Inspection Guide

## Daily inspection steps

Run the combined inspection:

```bash
./scripts/run-post-release-inspection.sh daily
```

This performs three checks:
1. shard audit recent window
2. shard metrics snapshot
3. cluster residual-state baseline

Hourly mode skips the heavier cluster residual check:

```bash
./scripts/run-post-release-inspection.sh hourly
```

Dry-run mode prints the planned inspection steps without executing them:

```bash
./scripts/run-post-release-inspection.sh daily --dry-run
```

Machine-readable output:

```bash
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/post-release.json ./scripts/run-post-release-inspection.sh daily
```

## Individual checks

Shard audit:

```bash
greenmqtt shard audit --limit 20 --output json
```

Shard metrics snapshot:

```bash
./scripts/run-shard-metrics-snapshot.sh
```

Cluster residual-state baseline:

```bash
./scripts/run-cluster-profile.sh ci
```

## Scheduled execution

Suggested split:
- hourly: `./scripts/run-post-release-inspection.sh hourly`
- daily: `./scripts/run-post-release-inspection.sh daily`

For automation or cron-like scheduling, persist JSON output:

```bash
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/post-release-hourly.json ./scripts/run-post-release-inspection.sh hourly
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/post-release-daily.json ./scripts/run-post-release-inspection.sh daily
```

## What healthy output looks like

- `post-release-inspection.status == 0`
- shard audit query returns successfully
- shard metrics snapshot returns structured counts for:
  - `mqtt_shard_move_total`
  - `mqtt_shard_failover_total`
  - `mqtt_shard_anti_entropy_total`
  - `mqtt_shard_fencing_reject_total`
- cluster CI profile remains green

## Escalation path

### Shard audit shows unexpected churn

Symptoms:
- repeated `shard_move` / `shard_failover`
- unexpected `shard_repair`

Escalation:
1. Inspect current ownership with `greenmqtt shard ls`.
2. Compare recent audit entries against expected rollout or maintenance activity.
3. If churn is unexplained, stop manual shard moves until owner health is confirmed.

### Fencing reject count grows

Symptoms:
- `mqtt_shard_fencing_reject_total` increases between inspections

Escalation:
1. Run `greenmqtt shard audit --limit 20 --output json`.
2. Identify which shard kind and tenant were involved.
3. Reconcile current owner using `greenmqtt shard ls`.
4. If needed, run the documented rollback or recovery drill before manual intervention.

### Cluster residual check fails

Symptoms:
- `./scripts/run-cluster-profile.sh ci` returns non-zero

Escalation:
1. Treat the node set as unstable.
2. Re-run the profile once to rule out transient host noise.
3. If it fails again, do not continue rollout or backend switch work.
4. Escalate to rollback using [rollback-drills.md](/Users/jizhenggang/Documents/mqtt/greenmqtt/docs/rollback-drills.md).

## Handoff notes

- Hourly mode is the safe default for scheduled automation.
- Daily mode is the handoff-grade inspection because it includes the cluster residual-state check.
- When handing this to another operator, pass along:
  - the `GREENMQTT_HTTP_BIND` value
  - the JSON summary output location
  - the rollback drill entry point
