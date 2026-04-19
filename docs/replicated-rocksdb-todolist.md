# Replicated RocksDB Architecture TODO

This document tracks the phased work needed to evolve `greenmqtt` from
`broker + local/shared backend` into a replicated shard architecture where
RocksDB is only the local storage engine.

## Target Shape

- `greenmqtt-broker` becomes the MQTT protocol and online-session front end.
- `sessiondict`, `dist`, `inbox`, and `retain` become replicated state services.
- shard ownership, leader routing, snapshots, and recovery are driven by a
  shared base-kv layer.
- RocksDB is reduced to a local data engine used by replicated ranges.

## Phase 1: Shared Metadata Foundation

- [x] Add base-kv vocabulary to `greenmqtt-core`:
  - [x] range id
  - [x] range boundary
  - [x] replica role and sync state
  - [x] replicated range descriptor
  - [x] helpers for leader lookup and boundary checks
- [x] Keep new metadata additive so current `ServiceShard*` users continue to compile.
- [x] Add focused unit tests for new metadata semantics.
- [x] Define naming rules for service shard keys versus split ranges.

Naming rules:

- `ServiceShardKey` remains the coarse service-level ownership unit used by current broker and
  recovery code.
- `ReplicatedRangeDescriptor` is the future split unit inside a service shard.
- A single `ServiceShardKey` may map to one or many replicated ranges.
- Range ids must be globally unique and stable across leader transfer.
- Range boundaries must use half-open semantics: `[start_key, end_key)`.

## Phase 2: Raft and Local Engine

- [x] Create `greenmqtt-kv-raft` crate.
- [x] Introduce raft node lifecycle:
  - [x] tick
  - [x] propose
  - [x] read index
  - [x] leadership transfer
  - [x] config change
  - [x] snapshot install
  - [x] recovery
- [x] Create `greenmqtt-kv-engine` crate.
- [x] Move RocksDB-specific local persistence behind engine interfaces:
  - [x] data engine
  - [x] WAL / log storage
  - [x] checkpoint and snapshot read/write
  - [x] compaction hooks
- [ ] Refactor current RocksDB business stores into range-scoped primitives instead of
  exposing `RocksSessionStore`, `RocksRouteStore`, `RocksInboxStore`, and `RocksRetainStore`
  as the final abstraction.

## Phase 3: Base-KV Service Layer

- [x] Create `greenmqtt-kv-server` crate to host replicated ranges behind RPC.
- [x] Create `greenmqtt-kv-client` crate for:
  - [x] range routing
  - [x] leader selection
  - [x] query-ready replica selection
  - [x] redirect/retry handling
- [x] Extend `greenmqtt-rpc` to expose generic range query/mutation/snapshot RPCs.
- [ ] Promote the current in-memory assignment registry into a real metadata service that
  tracks:
  - [x] range descriptors
  - [x] replica sets
  - [x] leader
  - [x] config version
  - [x] balancing state

## Phase 4: Cluster and Balancing

- [ ] Upgrade `greenmqtt-cluster` from membership shell to real cluster control:
  - [ ] service advertisement
  - [ ] node lifecycle transitions
  - [ ] failure detection integration
  - [ ] store join/leave workflows
- [ ] Add continuous balancing control similar to a base-kv balancer layer:
  - [ ] bootstrap
  - [ ] replica count balancing
  - [ ] leader balancing
  - [ ] split and merge
  - [ ] unreachable replica cleanup
  - [ ] recovery balancing

## Phase 5: Service Migration

- [ ] Migrate `retain` onto replicated ranges first.
- [ ] Migrate `dist` onto replicated ranges next.
- [ ] Migrate `sessiondict` onto replicated ranges.
- [ ] Migrate `inbox` and inflight tracking last.
- [ ] Preserve existing service-facing traits as facades during transition, then simplify.

## Phase 6: Broker Refactor

- [ ] Make broker depend on service clients only, not local durable implementations.
- [ ] Remove direct assumptions that durable state is local.
- [ ] Route mutation requests to range leaders.
- [ ] Restrict local state to:
  - [ ] live connections
  - [ ] transient send queues
  - [ ] short-lived admission and quota tracking

## Phase 7: Verification and Operations

- [ ] Add tests for:
  - [ ] three-replica write commit semantics
  - [ ] leader failover
  - [ ] node restart recovery
  - [ ] snapshot restore
  - [ ] range split / merge
  - [ ] broker reconnect after leader move
- [ ] Extend Helm and local-k8s deployment to model store topology separately from broker topology.
- [ ] Remove Redis as the default multi-node shared state path only after replicated ranges
  cover all four state services.

## Current Status Notes

- `greenmqtt-kv-raft` now has a reference in-memory raft node that exercises the
  lifecycle surface and keeps later integrations unblocked.
- `greenmqtt-kv-engine` now has a reference in-memory range engine and WAL store.
- `greenmqtt-kv-engine` now also has a RocksDB-backed range engine and WAL store with
  checkpoint/snapshot/compaction support.
- `greenmqtt-kv-client` now has an in-memory range router with leader and query-ready
  selection helpers.
- `greenmqtt-kv-server` now has an in-memory range host that can surface raft-backed
  range status.
- `greenmqtt-rpc` now keeps in-memory replicated range descriptors and balancer state
  alongside shard assignments and membership.
- `greenmqtt-rpc` now exposes gRPC metadata APIs plus generic range get/scan/apply/checkpoint/snapshot
  RPCs backed by the in-memory range host.
- `greenmqtt-retain` now includes a range-backed retain implementation that works over both
  local executors and the gRPC kv-range executor, but broker/runtime wiring still points to the
  legacy retain implementations.
- `greenmqtt-dist` now includes a range-backed dist implementation over kv-range, but broker/runtime
  wiring still points to the legacy dist implementations.
- RocksDB-backed range engine and real replicated raft behavior remain unfinished.
