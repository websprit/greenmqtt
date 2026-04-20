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
- [x] Refactor current RocksDB business stores into range-scoped primitives instead of
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
- [x] Promote the current in-memory assignment registry into a real metadata service that
  tracks:
  - [x] range descriptors
  - [x] replica sets
  - [x] leader
  - [x] config version
  - [x] balancing state

## Phase 4: Cluster and Balancing

- [x] Upgrade `greenmqtt-cluster` from membership shell to real cluster control:
  - [x] service advertisement
  - [x] node lifecycle transitions
  - [x] failure detection integration
  - [x] store join/leave workflows
- [x] Add continuous balancing control similar to a base-kv balancer layer:
  - [x] bootstrap
  - [x] replica count balancing
  - [x] leader balancing
  - [x] split and merge
  - [x] unreachable replica cleanup
  - [x] recovery balancing

## Phase 5: Service Migration

- [x] Migrate `retain` onto replicated ranges first.
- [x] Migrate `dist` onto replicated ranges next.
- [x] Migrate `sessiondict` onto replicated ranges.
- [x] Migrate `inbox` and inflight tracking last.
- [x] Preserve existing service-facing traits as facades during transition, then simplify.

## Phase 6: Broker Refactor

- [x] Make broker depend on service clients only, not local durable implementations.
- [x] Remove direct assumptions that durable state is local.
- [x] Route mutation requests to range leaders.
- [x] Restrict local state to:
  - [x] live connections
  - [x] transient send queues
  - [x] short-lived admission and quota tracking

## Phase 7: Verification and Operations

 - [x] Add tests for:
  - [x] three-replica write commit semantics
  - [x] leader failover
  - [x] node restart recovery
  - [x] snapshot restore
  - [x] range split / merge
  - [x] broker reconnect after leader move
- [x] Extend Helm and local-k8s deployment to model store topology separately from broker topology.
- [x] Remove Redis as the default multi-node shared state path only after replicated ranges
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
- `greenmqtt-rpc` now also has a persistent metadata registry backed by RocksDB kv-engine,
  and `serve` mode can opt into it with `GREENMQTT_METADATA_BACKEND=rocksdb`.
- `greenmqtt-rpc` now exposes gRPC metadata APIs plus generic range get/scan/apply/checkpoint/snapshot
  RPCs backed by the in-memory range host.
- `greenmqtt-kv-balance` now provides a minimal metadata controller that can bootstrap missing
  ranges, reconcile replica counts, rebalance leaders, clean unreachable replicas, recover
  leader-less ranges, split and merge adjacent ranges, fail over assignments from offline
  members, and persist balancer state through the control-plane registry.
- `greenmqtt-retain` now includes a range-backed retain implementation that works over both
  local executors and the gRPC kv-range executor, and CLI/runtime wiring now supports an opt-in
  replicated retain mode while legacy retain remains the default.
- `greenmqtt-dist` now includes a range-backed dist implementation over kv-range, but broker/runtime
  wiring now supports an opt-in replicated dist mode while legacy dist remains the default.
- `greenmqtt-sessiondict` now includes a range-backed session directory implementation, and
  CLI/runtime wiring now supports an opt-in replicated sessiondict mode while legacy sessiondict
  remains the default.
- `greenmqtt-inbox` now includes a range-backed inbox/inflight implementation, and CLI/runtime
  wiring now supports an opt-in replicated inbox mode while legacy inbox remains the default.
- `greenmqtt-cluster` now has a minimal coordinator that advertises local services, applies
  failure-detector membership changes into the control-plane registry, updates local lifecycle
  state, and can trigger join/leave workflows through the balance controller.
- CLI/runtime service selection now supports a shared `GREENMQTT_STATE_MODE` with per-service
  overrides, and that precedence is now covered by tests, which reduces the work needed to flip
  the default broker path to replicated services.
- A CLI integration test now proves that all four durable services can run together under
  `GREENMQTT_STATE_MODE=replicated` against a single metadata plus kv-range endpoint.
- `greenmqtt-kv-client` now has a leader-routed kv-range executor that resolves member endpoints
  from metadata and falls back to a direct range endpoint only when routing metadata is absent.
- `greenmqtt-rpc` metadata service now exposes cluster member CRUD/list operations, and a restart
  test now proves that persisted member plus range routing survives process restart.
- A broker integration test now proves that `BrokerRuntime` can run end-to-end on replicated
  service clients under `GREENMQTT_STATE_MODE=replicated`, instead of depending on local durable
  state implementations.
- CLI integration tests now also prove that a replicated retain client survives leader failover,
  and that a broker using replicated service clients can continue publishing retained messages
  after the retain leader moves to a new node and the old node goes away.
- `greenmqtt-retain` now has a replicated-range test that exercises split routing, merges the two
  ranges back into a single merged range, and verifies retained lookups still work afterwards.
- `greenmqtt-kv-engine` now supports restoring a range from a snapshot for both memory and
  RocksDB engines, and `greenmqtt-retain` now has a replicated-range test proving a restored
  snapshot range can still serve retained lookups after the router switches over.
- The `greenmqtt-cli` rocksdb backend now boots a local `RocksDbKvEngine` and serves
  `sessiondict/dist/inbox/retain` through replicated service facades over fixed local ranges,
  instead of wiring the old `Rocks*Store` business adapters into the broker path. A guard now
  rejects legacy-only rocksdb directory layouts instead of silently reading empty state.
- `greenmqtt-cli` now supports a `state-serve` mode that exposes state RPC without starting a
  broker or MQTT listeners. The Helm chart now has optional `storeTopology` rendering for a
  dedicated store StatefulSet and store Service, and the local 3-node k8s values now default to
  a broker topology backed by a separate RocksDB store pod instead of Redis.
- `MemoryRaftNode` now simulates quorum-gated commit progression for multi-voter clusters instead
  of committing immediately on `propose`, and `greenmqtt-kv-raft` now has a test proving a
  three-replica cluster only advances commit index after a majority acknowledgement.
- `greenmqtt-broker` now exposes a `LocalHotStateBreakdown` that explicitly counts only live
  connections, transient send-queue entries, and short-lived tracking markers. The debounce window
  now prunes stale recent-connect markers so that local admission tracking stays bounded, and
  broker tests now prove durable service records do not appear in local hot-state accounting.
- RocksDB-backed range engine and real replicated raft behavior remain unfinished.
