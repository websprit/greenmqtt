# Rollback Drill Guide

This guide is for on-call and handoff drills. Every command here is intended to be runnable without
editing source code.

## Prerequisites

- `GREENMQTT_HTTP_BIND` points at a reachable admin API for shard commands.
- Local Redis is available when the drill touches storage upgrade or cluster failover.
- Run from the repository root.

## Shard move rollback

1. Inspect the shard before the move:

   ```bash
   greenmqtt shard ls --kind inbox --tenant-id t1
   ```

2. Preview the move:

   ```bash
   greenmqtt shard move inbox t1 s1 9 --dry-run
   ```

3. Execute the move:

   ```bash
   greenmqtt shard move inbox t1 s1 9
   ```

4. Roll it back to the previous owner:

   ```bash
   greenmqtt shard move inbox t1 s1 7
   ```

5. Verify owner and audit:

   ```bash
   greenmqtt shard ls --kind inbox --tenant-id t1
   greenmqtt shard audit --limit 20
   ```

Expected result:
- owner returns to the original node
- audit shows both the forward move and the rollback move

## Cluster failover rollback

Use the deterministic cluster profile:

```bash
./scripts/run-cluster-profile.sh local
```

Interpretation:
- the failover drill is successful when the profile shows reconnect recovery without residual offline state
- if the profile fails, do not attempt a manual owner restoration until the cluster profile is green again

## Configuration rollback

Run the storage upgrade baseline before and after the config rollback:

```bash
./scripts/run-storage-upgrade-baseline.sh
```

Typical rollback flow:
1. switch `GREENMQTT_STORAGE_BACKEND` back to the last known-good backend
2. point `GREENMQTT_DATA_DIR` or `GREENMQTT_REDIS_URL` back to the matching state source
3. re-run `./scripts/run-storage-upgrade-baseline.sh`
4. confirm `session / route / retain` state is preserved

## One-command drill

For a full operator dry run, use:

```bash
./scripts/run-rollback-drill.sh
```

It exercises:
- shard move rollback baseline
- cluster failover rollback baseline
- configuration rollback baseline

The script prints JSON summary and returns non-zero on any failed drill.

## Recovery notes

### Rollback command rejected by fencing

1. Run `greenmqtt shard ls` again.
2. Identify the newest owner and fence.
3. Re-issue the rollback against the current assignment.

### Failover profile fails after node recovery

1. Inspect cluster profile output for residual-state leakage.
2. Re-run `./scripts/run-cluster-profile.sh local`.
3. Only attempt manual shard failover after the deterministic profile is green.

### Config rollback restores backend but state still diverges

1. Re-run `./scripts/run-storage-upgrade-baseline.sh`.
2. If it still fails, treat the backend switch as incomplete.
3. Use the last known-good backend and data source until the baseline is green again.
