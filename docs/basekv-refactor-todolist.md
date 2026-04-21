# GreenMQTT BaseKV-Style Refactor Todo List

This file is the execution checklist for the GreenMQTT range-client and typed-command refactor.

Execution rules:

- Complete tasks in order unless a task explicitly says it can run in parallel.
- Do not start a task until all of its dependencies are done.
- After each task, run the listed validation before moving on.
- Keep diffs small and reversible.
- Prefer deleting old call paths after the replacement path is verified.

## Progress Board

- [x] T01 Create crate and module boundaries
- [x] T02 Refactor `greenmqtt-kv-client` into core abstractions
- [x] T03 Add unified core client traits
- [x] T04 Add typed consistency and error model
- [x] T05 Add route cache and refresh policy
- [x] T06 Implement remote facade builder
- [x] T07 Migrate CLI to unified facade
- [ ] T08 Migrate HTTP API to unified control facade
- [x] T09 Migrate replicated retain handle to facade
- [x] T10 Migrate replicated sessiondict handle to facade
- [x] T11 Migrate replicated dist handle to facade
- [x] T12 Migrate replicated inbox handle to facade
- [ ] T13 Add retain typed internal range RPC
- [ ] T14 Add sessiondict typed internal range RPC
- [ ] T15 Add dist typed internal range RPC
- [ ] T16 Add inbox typed internal range RPC
- [ ] T17 Replace simple loop schedulers with range-aware batching
- [ ] T18 Remove obsolete primitive paths and harden tests

## T01 Create crate and module boundaries

Status: `done`

Goal:
Split pure client abstractions from gRPC transport code so later business crates depend only on stable interfaces.

Target crates:

- `crates/greenmqtt-kv-client`
- `crates/greenmqtt-range-client` (new)
- `Cargo.toml`

Target files:

- `greenmqtt/Cargo.toml`
- `greenmqtt/crates/greenmqtt-kv-client/Cargo.toml`
- `greenmqtt/crates/greenmqtt-range-client/Cargo.toml`
- `greenmqtt/crates/greenmqtt-range-client/src/lib.rs`

Actions:

- Add `greenmqtt-range-client` to the workspace.
- Keep `greenmqtt-kv-client` transport-free.
- Make `greenmqtt-range-client` depend on `greenmqtt-kv-client` and `greenmqtt-rpc`.
- Add placeholder module skeletons in the new crate.

Done when:

- `cargo check -p greenmqtt-kv-client -p greenmqtt-range-client` passes.
- There is no dependency cycle involving `greenmqtt-rpc`.

Depends on:

- none

Validation:

- `cargo check -p greenmqtt-kv-client -p greenmqtt-range-client`

## T02 Refactor `greenmqtt-kv-client` into core abstractions

Status: `done`

Goal:
Turn the current single-file implementation into maintainable modules before adding more logic.

Target crate:

- `crates/greenmqtt-kv-client`

Target files:

- `greenmqtt/crates/greenmqtt-kv-client/src/lib.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/router.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/executor.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/client.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/cache.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/error.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/consistency.rs` (new)
- `greenmqtt/crates/greenmqtt-kv-client/src/control.rs` (new)

Actions:

- Move routing types and traits into `router.rs`.
- Move execution traits into `executor.rs`.
- Move `LeaderRoutedKvRangeExecutor` into `client.rs`.
- Reserve `cache.rs`, `error.rs`, `consistency.rs`, and `control.rs` for later tasks.
- Keep public exports in `lib.rs` minimal and intentional.

Done when:

- Existing unit tests for `greenmqtt-kv-client` still pass.
- Public API compiles with no behavior change.

Depends on:

- T01

Validation:

- `cargo test -p greenmqtt-kv-client`

## T03 Add unified core client traits

Status: `done`

Goal:
Provide one stable entry point for data-plane and control-plane operations.

Target crate:

- `crates/greenmqtt-kv-client`

Target files:

- `greenmqtt/crates/greenmqtt-kv-client/src/client.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/control.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/lib.rs`

Actions:

- Add `RangeDataClient` trait.
- Add `RangeControlClient` trait.
- Add route helpers such as `route_by_id`, `route_by_key`, and `route_shard`.
- Add core methods for `get`, `scan`, `apply`, `checkpoint`, and `snapshot`.
- Add control methods for `bootstrap`, `change_replicas`, `transfer_leadership`, `recover`, `split`, `merge`, `drain`, and `retire`.

Done when:

- Business code can depend on `RangeDataClient` and `RangeControlClient` instead of `KvRangeRouter + KvRangeExecutor`.

Depends on:

- T02

Validation:

- `cargo check -p greenmqtt-kv-client`

## T04 Add typed consistency and error model

Status: `done`

Goal:
Make read semantics and retry behavior explicit instead of relying on implicit gRPC/status string behavior.

Target crate:

- `crates/greenmqtt-kv-client`

Target files:

- `greenmqtt/crates/greenmqtt-kv-client/src/consistency.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/error.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/client.rs`

Actions:

- Add `ReadConsistency` enum with at least `LocalMaybeStale`, `LeaderReadIndex`, and `LeaderWriteAck`.
- Add typed client error enum with at least `NotLeader`, `EpochMismatch`, `RangeNotFound`, `ConfigChanging`, `TransportUnavailable`, and `RetryExhausted`.
- Replace string-based retry classification with typed mapping.
- Thread consistency choices into `RangeDataClient` methods.

Done when:

- Retry logic no longer depends on raw message string parsing in public-facing code.
- Read behavior is explicit at call sites.

Depends on:

- T03

Validation:

- `cargo test -p greenmqtt-kv-client`

## T05 Add route cache and refresh policy

Status: `done`

Goal:
Centralize route refresh behavior so business modules stop doing ad hoc lookups.

Target crates:

- `crates/greenmqtt-kv-client`
- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/crates/greenmqtt-kv-client/src/cache.rs`
- `greenmqtt/crates/greenmqtt-kv-client/src/client.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`

Actions:

- Add cache entries for `range_id`, `shard+key`, and `shard`.
- Add TTL-based refresh.
- Refresh on `NotLeader`, `EpochMismatch`, and `RangeNotFound`.
- Expose explicit cache invalidation hooks for tests.

Done when:

- Route refresh policy is defined in one place.
- Business modules no longer need manual route refresh logic.

Depends on:

- T04

Validation:

- `cargo test -p greenmqtt-kv-client`
- `cargo test -p greenmqtt-range-client`

## T06 Implement remote facade builder

Status: `done`

Goal:
Hide direct use of `MetadataGrpcClient`, `KvRangeGrpcClient`, and `RoutedRangeControlGrpcClient` behind one builder.

Target crate:

- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/crates/greenmqtt-range-client/src/lib.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/builder.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/control_remote.rs`

Actions:

- Add a builder that constructs a remote `RangeDataClient`.
- Add a builder that constructs a remote `RangeControlClient`.
- Internally wire `MetadataGrpcClient`, `KvRangeGrpcClient`, `KvRangeGrpcExecutorFactory`, and `RoutedRangeControlGrpcClient`.
- Keep all transport-specific code in this crate.

Done when:

- Callers can create remote data/control clients without directly touching raw gRPC client types.

Depends on:

- T05

Validation:

- `cargo test -p greenmqtt-range-client`

## T07 Migrate CLI to unified facade

Status: `done`

Goal:
Remove manual client assembly from the CLI.

Target crate:

- `crates/greenmqtt-cli`

Target files:

- `greenmqtt/crates/greenmqtt-cli/src/main.rs`

Actions:

- Replace direct construction of `LeaderRoutedKvRangeExecutor`.
- Replace direct construction of `MetadataGrpcClient`, `KvRangeGrpcClient`, and `RoutedRangeControlGrpcClient`.
- Route all range data/control calls through the facade builder.

Done when:

- CLI has no direct knowledge of low-level range gRPC client wiring.

Depends on:

- T06

Validation:

- `cargo check -p greenmqtt-cli`
- Run CLI smoke path that creates remote range clients

## T08 Migrate HTTP API to unified control facade

Status: `done`

Goal:
Remove direct `RangeControlGrpcClient` usage from the HTTP API.

Target crate:

- `crates/greenmqtt-http-api`

Target files:

- `greenmqtt/crates/greenmqtt-http-api/src/range.rs`

Actions:

- Replace raw range control client construction with the unified control facade.
- Keep admin-only code separate if it does not belong in the facade.

Done when:

- HTTP API does not construct `RangeControlGrpcClient` directly.

Depends on:

- T06

Validation:

- `cargo check -p greenmqtt-http-api`

## T09 Migrate replicated retain handle to facade

Status: `done`

Goal:
Make retain depend on the unified data client rather than raw router/executor pair.

Target crate:

- `crates/greenmqtt-retain`

Target files:

- `greenmqtt/crates/greenmqtt-retain/src/lib.rs`

Actions:

- Change `ReplicatedRetainHandle` constructor to accept `Arc<dyn RangeDataClient>`.
- Remove direct `KvRangeRouter` and `KvRangeExecutor` fields from the replicated handle.
- Keep behavior unchanged for now.

Done when:

- Retain replicated path compiles and tests pass using only facade abstractions.

Depends on:

- T03
- T06

Validation:

- `cargo test -p greenmqtt-retain`

## T10 Migrate replicated sessiondict handle to facade

Status: `done`

Goal:
Make sessiondict depend on the unified data client rather than raw router/executor pair.

Target crate:

- `crates/greenmqtt-sessiondict`

Target files:

- `greenmqtt/crates/greenmqtt-sessiondict/src/lib.rs`

Actions:

- Change `ReplicatedSessionDictHandle` constructor to accept `Arc<dyn RangeDataClient>`.
- Remove direct `KvRangeRouter` and `KvRangeExecutor` fields from the replicated handle.
- Keep current behavior until typed RPC replacement lands.

Done when:

- Sessiondict replicated path compiles and tests pass using only facade abstractions.

Depends on:

- T03
- T06

Validation:

- `cargo test -p greenmqtt-sessiondict`

## T11 Migrate replicated dist handle to facade

Status: `done`

Goal:
Make dist depend on the unified data client rather than raw router/executor pair.

Target crate:

- `crates/greenmqtt-dist`

Target files:

- `greenmqtt/crates/greenmqtt-dist/src/dist/mod.rs`

Actions:

- Change `ReplicatedDistHandle` constructor to accept `Arc<dyn RangeDataClient>`.
- Remove direct `KvRangeRouter` and `KvRangeExecutor` fields from the replicated handle.
- Keep current behavior until typed RPC replacement lands.

Done when:

- Dist replicated path compiles and tests pass using only facade abstractions.

Depends on:

- T03
- T06

Validation:

- `cargo test -p greenmqtt-dist`

## T12 Migrate replicated inbox handle to facade

Status: `in progress`

Goal:
Make inbox depend on the unified data client rather than raw router/executor pair.

Target crate:

- `crates/greenmqtt-inbox`

Target files:

- `greenmqtt/crates/greenmqtt-inbox/src/lib.rs`

Actions:

- Change replicated inbox path to accept `Arc<dyn RangeDataClient>`.
- Remove direct `KvRangeRouter` and `KvRangeExecutor` fields from replicated inbox state.
- Keep behavior unchanged for now.

Done when:

- Inbox replicated path compiles and tests pass using only facade abstractions.

Depends on:

- T03
- T06

Validation:

- `cargo test -p greenmqtt-inbox`

## T13 Add retain typed internal range RPC

Status: `todo`

Goal:
Move retain match/write logic from client-side primitive composition into a typed internal service.

Target crates:

- `crates/greenmqtt-proto`
- `crates/greenmqtt-rpc`
- `crates/greenmqtt-retain`
- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/proto/greenmqtt_internal.proto`
- `greenmqtt/crates/greenmqtt-rpc/src/service_impls.rs`
- `greenmqtt/crates/greenmqtt-rpc/src/clients.rs`
- `greenmqtt/crates/greenmqtt-retain/src/lib.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`

Actions:

- Add retain range-internal RPC messages and service methods.
- Add a retain range-store trait in `greenmqtt-retain`.
- Implement RPC server and client adapters.
- Switch replicated retain path to typed retain RPC.

Done when:

- `retain` no longer uses raw `route_key + apply` or shard-wide `scan` for its hot path.

Depends on:

- T09

Validation:

- `cargo test -p greenmqtt-rpc replicated_retain_can_use_kv_range_grpc_executor -- --nocapture`
- `cargo test -p greenmqtt-retain`

## T14 Add sessiondict typed internal range RPC

Status: `todo`

Goal:
Move register/lookup/unregister logic from client-side primitive composition into a typed internal service.

Target crates:

- `crates/greenmqtt-proto`
- `crates/greenmqtt-rpc`
- `crates/greenmqtt-sessiondict`
- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/proto/greenmqtt_internal.proto`
- `greenmqtt/crates/greenmqtt-rpc/src/service_impls.rs`
- `greenmqtt/crates/greenmqtt-rpc/src/clients.rs`
- `greenmqtt/crates/greenmqtt-sessiondict/src/lib.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`

Actions:

- Add sessiondict typed range RPC.
- Add `SessionDictRangeStore` trait and a local implementation.
- Add remote client adapter.
- Switch replicated sessiondict path to typed RPC.

Done when:

- `sessiondict` no longer depends on `apply_grouped` and primitive fallback scans for its core path.

Depends on:

- T10

Validation:

- `cargo test -p greenmqtt-sessiondict`
- `cargo test -p greenmqtt-rpc grpc_round_trip_for_session_online_check_surface -- --nocapture`

## T15 Add dist typed internal range RPC

Status: `todo`

Goal:
Move route add/remove/match/list logic from primitive calls into a typed internal service.

Target crates:

- `crates/greenmqtt-proto`
- `crates/greenmqtt-rpc`
- `crates/greenmqtt-dist`
- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/proto/greenmqtt_internal.proto`
- `greenmqtt/crates/greenmqtt-rpc/src/service_impls.rs`
- `greenmqtt/crates/greenmqtt-rpc/src/clients.rs`
- `greenmqtt/crates/greenmqtt-dist/src/dist/mod.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`

Actions:

- Add dist typed range RPC.
- Add `DistRangeStore` trait and a local implementation.
- Add remote client adapter.
- Switch replicated dist path to typed RPC.

Done when:

- Dist match/list hot paths do not need tenant-wide primitive scans.

Depends on:

- T11

Validation:

- `cargo test -p greenmqtt-dist`
- `cargo test -p greenmqtt-rpc brokers_forward_cross_node_deliveries_over_grpc -- --nocapture`

## T16 Add inbox typed internal range RPC

Status: `todo`

Goal:
Move inbox fetch/ack/commit/expire logic from primitive scan-and-delete flows into a typed internal service.

Target crates:

- `crates/greenmqtt-proto`
- `crates/greenmqtt-rpc`
- `crates/greenmqtt-inbox`
- `crates/greenmqtt-range-client`

Target files:

- `greenmqtt/proto/greenmqtt_internal.proto`
- `greenmqtt/crates/greenmqtt-rpc/src/service_impls.rs`
- `greenmqtt/crates/greenmqtt-rpc/src/clients.rs`
- `greenmqtt/crates/greenmqtt-inbox/src/lib.rs`
- `greenmqtt/crates/greenmqtt-range-client/src/remote.rs`

Actions:

- Add inbox typed range RPC.
- Add `InboxRangeStore` trait and local implementation.
- Add remote client adapter.
- Switch replicated inbox path to typed RPC.

Done when:

- Inbox hot paths do not rely on shard-wide `scan` to remove or commit messages.

Depends on:

- T12

Validation:

- `cargo test -p greenmqtt-inbox`
- `cargo test -p greenmqtt-rpc grpc_round_trip_for_internal_services -- --nocapture`

## T17 Replace simple loop schedulers with range-aware batching

Status: `todo`

Goal:
Upgrade ad hoc loop schedulers to batch per range and per epoch.

Target crates:

- `crates/greenmqtt-sessiondict`
- `crates/greenmqtt-inbox`
- `crates/greenmqtt-retain`
- `crates/greenmqtt-dist`

Target files:

- `greenmqtt/crates/greenmqtt-sessiondict/src/lib.rs`
- `greenmqtt/crates/greenmqtt-inbox/src/lib.rs`
- new scheduler helper modules as needed

Actions:

- Replace loop-based `SessionOnlineCheckScheduler` implementation.
- Replace loop-based `InboxBatchScheduler` implementation.
- Add range-aware batching primitives for typed retain/dist commands where valuable.
- Batch by `range_id + expected_epoch + queue`.

Done when:

- Batch schedulers perform grouped requests instead of one RPC or one storage call per input item.

Depends on:

- T13
- T14
- T15
- T16

Validation:

- `cargo test -p greenmqtt-sessiondict`
- `cargo test -p greenmqtt-inbox`

## T18 Remove obsolete primitive paths and harden tests

Status: `todo`

Goal:
Delete compatibility code once the new path is stable, and lock in the new behavior.

Target crates:

- `crates/greenmqtt-kv-client`
- `crates/greenmqtt-range-client`
- `crates/greenmqtt-rpc`
- `crates/greenmqtt-retain`
- `crates/greenmqtt-sessiondict`
- `crates/greenmqtt-dist`
- `crates/greenmqtt-inbox`

Actions:

- Delete old constructors that expose raw router/executor combinations where no longer needed.
- Delete compatibility primitive paths that are now replaced by typed RPC.
- Remove obsolete full-scan helper flows from business crates.
- Add regression tests for route cache refresh, epoch fence handling, split/merge recovery, and typed RPC behavior.
- Add metrics assertions where practical.

Done when:

- Hot paths use typed range operations.
- Primitive `KvRangeService` remains as a low-level substrate only.
- The codebase no longer has duplicated old and new replicated paths.

Depends on:

- T17

Validation:

- `cargo test -p greenmqtt-kv-client -p greenmqtt-range-client -p greenmqtt-rpc -p greenmqtt-retain -p greenmqtt-sessiondict -p greenmqtt-dist -p greenmqtt-inbox`
- `cargo check --workspace`

## Notes

- Do not try to build a fully generic BifroMQ-style co-proc layer in the first pass.
- Prefer narrow typed range APIs first, then extract shared batching patterns after at least retain, inbox, and sessiondict are stable.
- If a task uncovers a crate dependency cycle, stop and fix the boundary instead of working around it inside business logic.
