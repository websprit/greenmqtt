# Production Multi-Raft Migration and Rollout Plan

This document is the operator-side migration plan for moving `greenmqtt`
from legacy local/shared state layouts into the replicated multi-raft store
plane.

## Supported Targets

- target store plane: replicated range-backed state over the multi-raft stack
- target broker mode: `GREENMQTT_STATE_MODE=replicated`
- target local durable engine: range-engine RocksDB layout

## Legacy Local Sled Migration

1. Freeze writes or schedule a short maintenance window for the tenant set being migrated.
2. Start a new replicated store plane and verify range health through `RangeAdminService`.
3. Export legacy `sessiondict/dist/inbox/retain` data from sled-backed services through the existing service APIs or an offline export job.
4. Import the exported records into the replicated ranges in this order:
   - `retain`
   - `dist`
   - `sessiondict`
   - `inbox`
5. Verify record counts and spot-check broker-visible behavior.
6. Flip brokers to `GREENMQTT_STATE_MODE=replicated` and point them at the replicated metadata/range endpoints.
7. Keep legacy sled data read-only until rollback risk is gone, then retire it.

## Legacy Local RocksDB Migration

1. Detect whether the node still uses the old per-store layout (`sessions/routes/...`) or the newer `range-engine` layout.
2. If the old layout is present, do not attempt live reuse.
3. Export records from the legacy layout offline.
4. Re-import into the new replicated range-engine store plane.
5. Only after import and verification, switch brokers to replicated mode.

Current guard:
- the CLI already rejects the legacy per-store RocksDB layout when `GREENMQTT_STORAGE_BACKEND=rocksdb` expects the `range-engine` layout.

## Redis-Backed Shared State Migration

1. Treat Redis as a legacy shared-state source, not as part of the target multi-raft store plane.
2. Snapshot Redis-backed `sessiondict/dist/inbox/retain` via service APIs or a dedicated export job.
3. Import that snapshot into the replicated range store plane.
4. Cut brokers over to replicated mode after range health and data counts match.
5. Keep Redis available for rollback during the cutover window only.

## Mixed-Mode Rollout

### Old broker + new store

Supported only through the existing gRPC service compatibility layer.

- old brokers must continue using the legacy service RPCs
- new store nodes must expose compatible service surfaces
- do not point old brokers directly at raw raft transport endpoints

### New broker + old store

Supported only in `GREENMQTT_STATE_MODE=legacy`.

- new brokers may continue talking to legacy local/shared state backends
- this is a temporary compatibility posture, not the target production shape

### Cluster Upgrade Sequencing

1. Bring up new replicated store nodes first.
2. Verify metadata, range health, and replica lag through admin APIs.
3. Migrate state data offline or during a short write freeze.
4. Flip a small broker canary set to `GREENMQTT_STATE_MODE=replicated`.
5. Observe broker retries, redirect behavior, and range health.
6. Roll the remaining brokers.
7. Retire legacy state backends only after rollback is no longer needed.

## Explicit Incompatibility Guards

The current codebase already enforces these guards:

- legacy per-store RocksDB layout is rejected when the new range-engine layout is required
- replicated service mode requires explicit metadata/range endpoints, so brokers do not silently
  "half-switch" into a fake local multi-raft mode
- stale epoch and non-serving ranges are rejected in the `KvRangeService` data path

Operational rule:
- if a migration path would require live dual-writes between a legacy backend and the replicated
  store plane, treat it as unsupported unless a dedicated migration tool is added later
