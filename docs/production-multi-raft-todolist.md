# Production Multi-Raft TODO

This document tracks the additional work required to turn the current
`greenmqtt` reference multi-range / reference-raft stack into a production-ready
multi-raft control and data plane.

## Current Baseline

- `greenmqtt-kv-raft` has a reference `MemoryRaftNode`.
- quorum-gated commit progression is now simulated for multi-voter clusters.
- `MemoryRaftNode` now also supports a pluggable `RaftStateStore`, and the in-memory
  store path is covered by restart-style tests.
- raft local state now persists through both memory-backed and RocksDB-backed state stores.
- `greenmqtt-kv-engine`, `greenmqtt-kv-server`, `greenmqtt-kv-client`, and the four
  state services already expose the right architectural seams.

That is enough for architecture and integration work, but not enough for a durable,
fault-tolerant production raft layer.

## Current Progress Notes

- `greenmqtt-kv-raft` now has both `MemoryRaftStateStore` and `RocksDbRaftStateStore`.
- `MemoryRaftNode` now persists and restores raft local state through `RaftStateStore`.
- restart-style tests now cover both memory-backed and RocksDB-backed raft local state restore.
- the RocksDB-backed store now persists hard state, progress state, snapshot metadata, and log
  entries under separate keys instead of serializing one monolithic range blob.
- `MemoryRaftNode` now has a reference election state machine with election timeout ticks,
  candidate transitions, persisted `voted_for`, vote bookkeeping, and higher-term stepdown.
- `MemoryRaftNode` now also drives heartbeat cadence, next-index / match-index follower progress,
  log conflict rewind on followers, and outbox-based AppendEntries / RequestVote exchange that is
  rich enough to exercise reference log replication behavior in tests.
- `greenmqtt-rpc` now gates `KvRangeService.get/scan/checkpoint/snapshot` behind raft
  `read_index()` so follower reads are rejected, and leader reads require an active leader lease.
- learner replicas now participate in the reference replication round, and tests cover learner
  catch-up plus subsequent promotion into the voter set through config change.
- `greenmqtt-rpc` now exposes a generic `RaftTransportService.Send` RPC that can carry
  AppendEntries, RequestVote, InstallSnapshot, and their response messages to a target range.
- `greenmqtt-kv-server` now has a `ReplicaTransport` abstraction plus a `ReplicaRuntime` skeleton
  that can tick hosted ranges and drain/send outbound raft messages.
- `ReplicaRuntime` now also has a bounded pending queue plus timeout-based retry behavior for
  outbound messages, which is enough for a first transport/runtime lane without yet providing a
  full production scheduler.
- `ReplicaRuntime` now also has an ordered `apply_once()` path that drains committed raft entries,
  decodes deterministic KV mutation batches, applies them to the range state machine, and persists
  `applied_index`. `KvRangeRpc.apply` now waits for the proposed command to become committed and
  applied before returning success.
- `ReplicaRuntime` can now also run as a background task loop through `spawn()`, so
  `greenmqtt-kv-server` is no longer limited to static hosted ranges plus manual test-time ticks.
- `greenmqtt-kv-engine` snapshots now carry a structured manifest with range id, term, index,
  boundary, checksum, and layout version, and RocksDB snapshots persist that manifest alongside
  checkpointed data.
- `greenmqtt-rpc` raft transport tests now cover `InstallSnapshot` message delivery into a target
  replica, so snapshot install has a working transport-level/reference path even though
  production-grade snapshot generation and truncation safety are still unfinished.
- replicated `retain`, `dist`, and `sessiondict` paths now have explicit idempotence tests for
  reapplying the same logical record, and replicated `inbox` now restores its offline sequence
  counter from stored state so restart does not overwrite earlier offline messages.
- `MemoryRaftNode` now maintains a recoverable committed-entry queue, and `ReplicaRuntime` now
  drives both `snapshot_once()` and `compact_once()` so the reference runtime can snapshot applied
  state metadata and safely compact logs only after a snapshot safety point exists.
- service-level replay tests now cover idempotent re-application for replicated `retain`, `dist`,
  `sessiondict`, and offline-message replay in `inbox`, so restart/replay of the same deterministic
  command batch does not fan out into duplicate durable state.
- `ReplicaRuntime` now also exposes direct raft control helpers (`change_replicas`,
  `transfer_leadership`, `recover_range`) that execute against hosted raft nodes, which lays the
  groundwork for wiring balancer commands into real replica state transitions.

## Phase 1: Durable Raft Local State

- [x] Introduce a durable `RaftStateStore` abstraction in `greenmqtt-kv-raft`.
- [x] Persist stable/local raft state across node recreation for the reference implementation:
  - [x] current term
  - [x] cluster config
  - [x] log entries
  - [x] latest snapshot metadata
  - [x] commit/applied progress
- [x] Add a RocksDB-backed `RaftStateStore`.
- [x] Separate raft hard state, raft log, and volatile progress tracking instead of storing one
  monolithic serialized snapshot.
- [x] Persist `voted_for` and election metadata explicitly once real leader election exists.

## Phase 2: Real Raft Protocol

- [x] Replace the reference `MemoryRaftNode` behavior with a real raft state machine:
  - [x] randomized election timeout
  - [x] candidate election
  - [x] vote bookkeeping
  - [x] heartbeat cadence
  - [x] next-index / match-index follower progress
  - [x] log conflict detection and rewind
  - [x] leader stepdown on newer term
- [x] Support log replication from leader to followers over actual message exchange.
- [x] Support read safety beyond local commit index shortcuts:
  - [x] `ReadIndex` or leader lease semantics
  - [x] follower read gating
- [x] Support learner catch-up and promotion.

## Phase 3: Transport and Replica Runtime

- [x] Add an explicit raft transport layer between store replicas:
  - [x] append entries RPC
  - [x] vote RPC
  - [x] install snapshot RPC
  - [x] backpressure / retry / timeout handling
- [ ] Add a replica runtime that drives:
  - [x] raft ticks
  - [x] outbound replication
  - [x] apply loop
  - [x] snapshot generation / install
- [x] Make `greenmqtt-kv-server` host replica tasks, not just static `HostedRange` handles.

## Phase 4: Apply Pipeline and Deterministic State Machine

- [ ] Split raft log replication from state machine apply:
  - [x] committed log queue
  - [x] ordered apply worker
  - [x] persisted applied index
- [x] Guarantee deterministic apply for every business store (`retain/dist/sessiondict/inbox`).
- [x] Reject client-visible success before the command is committed and applied.
- [x] Add idempotent re-apply guarantees for restart / replay.

## Phase 5: Snapshot and Log Compaction

- [ ] Implement snapshot generation from applied state, not only engine-level filesystem copies.
- [x] Define a snapshot manifest format:
  - [x] range id
  - [x] term
  - [x] index
  - [x] boundary
  - [x] checksum
  - [x] payload layout version
- [x] Implement snapshot install flow for lagging replicas.
- [x] Gate log truncation on installed snapshot safety.
- [x] Add periodic log compaction policy.

## Phase 6: Replica Membership and Reconfiguration

- [ ] Make replica changes go through raft joint-consensus style transitions or equivalent safe staging.
- [ ] Prevent balancer-issued replica changes from taking effect only in metadata.
- [ ] Ensure `greenmqtt-kv-balance` commands translate into real raft config change operations.
- [ ] Block unsafe leader transfers and replica removals while catch-up is incomplete.

## Phase 7: Range Lifecycle and Sharding

- [ ] Turn `split` and `merge` from metadata-only edits into data-moving range operations.
- [ ] Implement split workflow:
  - [ ] fence source range
  - [ ] create child ranges
  - [ ] copy / snapshot data into children
  - [ ] switch routing atomically
- [ ] Implement merge workflow:
  - [ ] fence source siblings
  - [ ] produce merged snapshot
  - [ ] install merged range
  - [ ] retire source ranges
- [ ] Persist range epoch / fencing semantics in the data path, not only metadata.

## Phase 8: Client Routing and Request Safety

- [ ] Make `greenmqtt-kv-client` refresh and retry against raft-specific redirect errors.
- [ ] Distinguish:
  - [ ] `NotLeader`
  - [ ] `EpochMismatch`
  - [ ] `RangeNotFound`
  - [ ] `ConfigChanging`
  - [ ] `SnapshotInProgress`
- [ ] Add bounded retry policy and caller-visible error categories.
- [ ] Ensure broker-facing service facades never silently succeed against stale leaders.

## Phase 9: Observability and Operations

- [ ] Add per-range raft metrics:
  - [ ] current term
  - [ ] role
  - [ ] leader id
  - [ ] commit index
  - [ ] applied index
  - [ ] replication lag per follower
  - [ ] snapshot send/install counters
  - [ ] election / stepdown counters
- [ ] Expose admin APIs for replica/range health.
- [ ] Add debug dumps for stuck ranges and replication progress.

## Phase 10: Failure Testing

- [ ] Add deterministic tests for:
  - [ ] leader crash before commit
  - [ ] leader crash after quorum commit but before apply
  - [ ] follower restart during catch-up
  - [ ] partitioned leader stepdown
  - [ ] split-brain prevention
  - [ ] joint-consensus membership changes
  - [ ] snapshot install after log truncation
- [ ] Add long-running chaos / soak coverage with 3+ store replicas.

## Phase 11: Rollout and Migration

- [ ] Add a migration plan from:
  - [ ] legacy local sled
  - [ ] legacy local rocksdb
  - [ ] redis-backed shared state
- [ ] Define mixed-mode rollout:
  - [ ] old broker + new store
  - [ ] new broker + old store
  - [ ] cluster upgrade sequencing
- [ ] Add explicit incompatibility guards where safe live migration is not possible.

## Acceptance Bar

The stack should only be considered production-ready when all of the following are true:

- a 3-replica range survives leader loss without data loss
- committed writes are never lost after quorum acknowledgement
- followers can restart from durable raft state and catch up
- snapshots can replace long log replay for lagging replicas
- balancer actions translate into real replica state transitions
- broker-facing service calls survive leader movement through retry/redirect behavior
- observability is strong enough to debug stuck or unhealthy replicas in production
