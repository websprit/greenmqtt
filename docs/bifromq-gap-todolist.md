# greenmqtt vs BifroMQ Gap TODO

This document compares the current `greenmqtt` codebase against the
`mqtt_brokers/bifromq` implementation by module, and records the missing
production-facing capabilities that are still worth building if the goal is to
approach BifroMQ's maturity.

Scope:
- compare only the modules requested here:
  - `sessiondict`
  - `dist`
  - `inbox`
  - `retain`
  - `mqtt front-door`
  - `control plane`
- keep the TODO focused on gaps, not on features already present in
  `greenmqtt`

Comparison anchors:
- `greenmqtt`
  - `crates/greenmqtt-sessiondict/src/lib.rs`
  - `crates/greenmqtt-dist/src/dist/mod.rs`
  - `crates/greenmqtt-inbox/src/lib.rs`
  - `crates/greenmqtt-retain/src/lib.rs`
  - `crates/greenmqtt-kv-client/src/lib.rs`
  - `crates/greenmqtt-kv-server/src/lib.rs`
  - `crates/greenmqtt-rpc/src/lib.rs`
  - `crates/greenmqtt-cli/src/main.rs`
- `BifroMQ`
  - `bifromq-session-dict/.../SessionDictServer.java`
  - `bifromq-dist/.../DistWorker.java`
  - `bifromq-inbox/.../InboxStore.java`
  - `bifromq-retain/.../RetainStore.java`
  - `base-kv/.../BaseKVMetaService.java`
  - `base-kv/.../BaseKVStoreClient.java`
  - `bifromq-mqtt/.../MQTTBroker.java`
  - `bifromq-mqtt/.../ConnectionRateLimitHandler.java`
  - `bifromq-mqtt/.../ConditionalSlowDownHandler.java`

## 1. SessionDict

### Current `greenmqtt`

- already has:
  - local, persistent, and replicated `SessionDirectory` implementations
  - replicated range-backed `register / unregister / lookup / list`
  - runtime/CLI wiring into the replicated state path
- current weakness:
  - replicated lookup still depends on coarse tenant scans in some paths
  - there is no dedicated `sessiondict` service layer with rich online-check /
    session-registration workflows
  - there is no specialized client-side batching/scheduling equivalent

### BifroMQ side

- `sessiondict` is a dedicated server/client subsystem, not just a KV facade
- it has separate registration and online-check client/server workflows
- the server is tightly integrated with broker/inbox workflows

### TODO

- [x] Add dedicated online-check and session-existence APIs on top of the current range-backed sessiondict path.
- [x] Add specialized client-side batching/scheduling for session online checks instead of one-call-per-lookup.
- [x] Replace the remaining scan-heavy lookup paths with explicit secondary indexes for:
  - [x] `tenant/user/client -> session`
  - [x] `session_id -> session`
- [x] Add direct broker/sessiondict integration for takeover, duplicate-client fencing, and online-state transitions rather than routing everything through generic KV lookups.
- [x] Add load/recovery tests for large per-tenant session populations so sessiondict is validated as an operational subsystem, not only a correctness layer.

## 2. Dist

### Current `greenmqtt`

- already has:
  - local, persistent, and replicated `DistRouter`
  - multi-raft/range-backed routing
  - split/merge and epoch fencing at the range/runtime layer
- current weakness:
  - replicated `dist` still reconstructs behavior from range scans and then
    rebuilds in-memory route views on demand
  - route matching is not yet a store-side optimized coprocessor/index path
  - delivery fanout orchestration is much thinner than BifroMQ's worker model

### BifroMQ side

- `dist-worker` is a dedicated base-kv store worker with:
  - coprocessor/schema-specific data path
  - balancer factories
  - cleaner jobs
  - dedicated message delivery integration
  - topic trie / cache / route-group machinery

### TODO

- [x] Move route matching from client-side scan-and-rebuild into a store-side indexed execution path.
- [x] Add a route schema that supports direct match lookups without full-range scans.
- [x] Add dist-specific cache layers for hot topic filters / route groups / fanout targets.
- [x] Add worker-style fanout and delivery pipelines instead of keeping `dist` as a mostly query-side facade.
- [x] Add dist-specific balancing policies comparable to BifroMQ's worker balancer factories.
- [x] Add dist cleanup / background maintenance jobs instead of relying only on generic runtime loops.

## 3. Inbox

### Current `greenmqtt`

- already has:
  - replicated inbox/offline/inflight support
  - deterministic replay coverage
  - restart-safe offline sequence continuation
- current weakness:
  - many subscription/offline/inflight flows still scan ranges directly
  - store-side delayed task handling is missing
  - there is no BifroMQ-level scheduler lattice for attach/detach/fetch/commit/LWT
  - QoS2 coverage is much thinner than BifroMQ's dedicated store test matrix

### BifroMQ side

- inbox is one of the most mature subsystems:
  - dedicated store/server/client split
  - delayed task runner
  - LWT send/retry path
  - tenant GC runner
  - many batch schedulers
  - deeper QoS0/1/2 test coverage

### TODO

- [x] Add explicit inbox delayed-task infrastructure for:
  - [x] delayed LWT sending
  - [x] inbox expiry
  - [x] retryable delayed work
- [x] Add tenant-level GC / expiry runners rather than relying only on generic scans.
- [x] Add inbox-side batching/schedulers for:
  - [x] attach/detach
  - [x] fetch
  - [x] commit/ack
  - [x] insert
  - [x] sub/unsub
- [x] Add a stronger QoS2-specific durable state machine and test matrix equivalent to BifroMQ's dedicated QoS tests.
- [x] Replace scan-heavy inbox/offline/inflight fetch paths with schema-aware indexes or coprocessor logic.
- [x] Add inbox-specific balancing policies and maintenance workers.

## 4. Retain

### Current `greenmqtt`

- already has:
  - replicated retain path
  - snapshot restore
  - split/merge-capable range runtime beneath it
- current weakness:
  - replicated retain still leans on straightforward KV scans for wildcard work
  - there is no retain-specific background GC processor / maintenance loop at
    the service layer
  - there is no retain-specific balancing/plugin ecosystem

### BifroMQ side

- retain is a dedicated store with:
  - its own co-proc path
  - balancer factories
  - GC processor
  - store lifecycle separate from generic KV runtime

### TODO

- [x] Add retain-specific indexed match acceleration so wildcard matching does not depend on broad range scans.
- [x] Add retain-specific background GC / compaction / cleanup processing at the service level.
- [x] Add retain-specific balancing policies instead of only using generic range balancing.
- [x] Add retain-focused load tests for large wildcard fanout and retained-topic cardinality.

## 5. MQTT Front-Door

### Current `greenmqtt`

- already has:
  - TCP/TLS/WS/WSS/QUIC listeners
  - MQTT packet parsing and session/message expiry handling
  - a usable broker/runtime path on top of the new state plane
- current weakness:
  - no BifroMQ-equivalent connection-rate limiting handler
  - no proxy-protocol support
  - no conditional slowdown / memory-pressure throttle path equivalent
  - no explicit dedup cache comparable to BifroMQ's inbound duplicate protection
  - listener hardening and ingress policy are much lighter

### BifroMQ side

- BifroMQ's front-door has dedicated handlers for:
  - connection rate limiting
  - conditional slowdown and throttled disconnect
  - proxy protocol
  - websocket-specific guards
  - QUIC-specific stream handlers
  - dedup cache

### TODO

- [x] Add connection-rate limiting as a first-class ingress handler.
- [x] Add Proxy Protocol support for deployments behind L4 proxies/load balancers.
- [x] Add conditional slowdown / resource-pressure backoff before hard disconnect.
- [x] Add inbound dedup protection for duplicate publish/session edge cases.
- [x] Add front-door traffic-shaping and ingress backpressure policies at the listener pipeline level.
- [x] Expand QUIC/WebSocket listener hardening and production diagnostics.

## 6. Control Plane

### Current `greenmqtt`

- already has:
  - persistent metadata registry
  - range admin gRPC
  - balancer/controller skeleton
  - runtime-executed config changes, split/merge, fencing, redirect-aware client
- current weakness:
  - metadata is still a local registry model exposed over RPC, not a distributed
    CRDT-style control plane
  - store client/control client is not yet as full-featured as BifroMQ's
    `BaseKVStoreClient`
  - safe staging exists, but not a fully formal joint-consensus implementation
  - orchestration is still lighter than BifroMQ's store ecosystem

### BifroMQ side

- BifroMQ has:
  - distributed base-kv meta service
  - a richer operational store client covering bootstrap/recover/split/merge/
    config change/leadership transfer/query/mutation
  - mature balancer-state reporting/proposal flows

### TODO

- [x] Replace the single-registry control-plane model with a distributed metadata service instead of a single persisted registry endpoint.
- [x] Add a richer store-control client comparable to `BaseKVStoreClient` for:
  - [x] bootstrap
  - [x] recover
  - [x] split
  - [x] merge
  - [x] transfer leadership
  - [x] change replica config
- [x] Tighten safe staging into a stricter joint-consensus-grade reconfiguration protocol.
- [x] Add controller feedback loops so balancer decisions, execution results, and health state form a closed operational loop.
- [x] Add store-level operator workflows for zombie detection, recovery, and forced retirement.

## Priority Order

If the goal is “be much closer to BifroMQ in real production behavior”, the
highest-value order is:

1. inbox
2. dist
3. control plane
4. mqtt front-door
5. sessiondict
6. retain

Reason:
- `inbox` and `dist` dominate correctness, fanout, replay, and delivery behavior
- control plane and front-door decide whether the system survives real traffic
  and real operational churn
- `sessiondict` and `retain` matter, but they are not the hottest failure or
  throughput surface compared with `inbox/dist`
