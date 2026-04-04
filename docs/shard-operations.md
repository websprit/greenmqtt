# Shard Operations Manual

## Scope

This manual covers the shard control plane exposed by `greenmqtt shard ...` and the matching
HTTP endpoints under `/v1/shards`.

Use these commands with `GREENMQTT_HTTP_BIND=<host:port>` pointing at the active admin API.

## Inspect current ownership

List all shard assignments:

```bash
greenmqtt shard ls
```

Filter by kind or tenant:

```bash
greenmqtt shard ls --kind inbox --tenant-id t1
```

Read shard-only audit history:

```bash
greenmqtt shard audit --limit 20
```

Use `--output json` when automation needs raw response payloads.

## Drain

Preview:

```bash
greenmqtt shard drain inbox t1 s1 --dry-run
```

Execute:

```bash
greenmqtt shard drain inbox t1 s1
```

Expected result:
- `previous` stays in `Serving`
- `current` moves to `Draining`
- audit records a `shard_drain` action

Use drain when you want the current owner to stop taking fresh work before move or failover.

## Move

Preview:

```bash
greenmqtt shard move inbox t1 s1 9 --dry-run
```

Execute:

```bash
greenmqtt shard move inbox t1 s1 9
```

Expected result:
- `previous.owner` is the old node
- `current.owner` is the target node
- `epoch` and `fencing_token` advance
- audit records `shard_move`

Run `greenmqtt shard ls --kind inbox --tenant-id t1` after the move to confirm the owner change.

## Catch-up

Preview:

```bash
greenmqtt shard catch-up inbox t1 s1 9 --dry-run
```

Execute:

```bash
greenmqtt shard catch-up inbox t1 s1 9
```

Use catch-up when the target already exists and must pull owner state before it can serve reads
or become a candidate owner.

Expected result:
- lifecycle stays `Recovering` or moves closer to `Serving`
- checksum-based state sync completes without changing owner
- audit records `shard_catch_up`

## Repair

Preview:

```bash
greenmqtt shard repair dist t1 tenant --dry-run
```

Execute:

```bash
greenmqtt shard repair dist t1 tenant 9
```

Use repair when replica checksum diverges from owner checksum and you want anti-entropy to
reconcile state without moving ownership.

Expected result:
- owner stays unchanged
- replica state converges
- audit records `shard_repair`

## Failover

Preview:

```bash
greenmqtt shard failover inbox t1 s1 9 --dry-run
```

Execute:

```bash
greenmqtt shard failover inbox t1 s1 9
```

Use failover when the owner is offline or cannot resume serving in time.

Expected result:
- new owner becomes the target node
- lifecycle returns to `Serving`
- audit records `shard_failover`

## Common errors and recovery

### Stale fencing / epoch mismatch

Symptoms:
- control action rejected
- snapshot stream rejected with `FailedPrecondition`
- shard audit shows newer `epoch` or `fencing_token` than the command you issued against

Recovery:
1. Re-run `greenmqtt shard ls --kind <kind> --tenant-id <tenant>` to refresh owner metadata.
2. Check `greenmqtt shard audit --limit 20` to find the latest successful transition.
3. Re-run the command against the current owner or current recovery target.

### Target node not serving the shard kind

Symptoms:
- move, catch-up, repair, or failover returns “missing endpoint”

Recovery:
1. Confirm target membership in `/v1/shards` or cluster membership output.
2. Ensure the node exposes the service endpoint for that shard kind.
3. Retry after the node rejoins in `Serving`.

### Dry-run looks correct but real action fails

Symptoms:
- `--dry-run` preview is valid
- non-dry-run action fails because state changed in between

Recovery:
1. Re-run the same command with `--dry-run`.
2. Compare `previous/current` preview with fresh `greenmqtt shard ls`.
3. Execute again only after the owner, epoch, and target are still current.

### Replica checksum never converges

Symptoms:
- repeated `repair` runs keep returning the same divergence

Recovery:
1. Run `greenmqtt shard repair ... --output json` and inspect `previous/current`.
2. Check shard metrics:
   - `mqtt_shard_anti_entropy_total`
   - `mqtt_shard_fencing_reject_total`
3. If divergence persists, run `catch-up`, then retry `repair`.
4. Escalate to a full `move` or `failover` if the owner is unhealthy.
