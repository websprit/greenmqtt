# Smoke and Profile Guide

## Local execution

Workspace smoke:

```bash
./scripts/run-workspace-smoke.sh
```

Performance profile:

```bash
./scripts/run-performance-profile.sh
```

Cluster profile:

```bash
./scripts/run-cluster-profile.sh local
./scripts/run-cluster-profile.sh ci
```

Storage backend upgrade baseline:

```bash
./scripts/run-storage-upgrade-baseline.sh
```

Compatibility matrix:

```bash
./scripts/run-compatibility-matrix.sh
```

Release checklist:

```bash
./scripts/run-release-checklist.sh
./scripts/run-release-checklist.sh --execute
```

Verification suite:

```bash
./scripts/run-verification-suite.sh quick
./scripts/run-verification-suite.sh full
```

Release-mode variants:

```bash
./scripts/run-performance-profile.sh --release
./scripts/run-cluster-profile.sh ci --release
./scripts/run-verification-suite.sh full --release
./scripts/run-compatibility-matrix.sh --release
```

## CI execution

Use JSON summary output for machine-readable status:

```bash
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/workspace.json ./scripts/run-verification-suite.sh full
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/cluster.json ./scripts/run-cluster-profile.sh ci
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/perf.json ./scripts/run-performance-profile.sh
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/storage.json ./scripts/run-storage-upgrade-baseline.sh
GREENMQTT_PROFILE_SUMMARY_FILE=/tmp/compat.json ./scripts/run-compatibility-matrix.sh
```

Every profile returns:
- `status: 0` on success
- `status: 1` when any step fails
- `results[*].name` for the logical step name
- `results[*].duration_seconds` for rough timing

## Result interpretation

### Workspace smoke

Expected coverage:
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cluster ci profile`

If smoke fails:
1. Fix clippy or test breakage first.
2. Re-run smoke before performance or release profiles.

### Performance profile

Expected coverage:
- metrics overhead gate
- backpressure overhead gate
- outbound bandwidth limit gate

If performance fails:
1. Re-run once to rule out host noise.
2. Compare the JSON summary durations between the last green run and the failing run.
3. Do not promote to release mode until `status: 0`.

RocksDB tuning environment variables:
- `GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES`
- `GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES`
- `GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES`
- `GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY`
- `GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS`
- `GREENMQTT_ROCKSDB_PARALLELISM`
- `GREENMQTT_ROCKSDB_BYTES_PER_SYNC`
- `GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS`
- `GREENMQTT_ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING`
- `GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO`

Use these only when `GREENMQTT_STORAGE_BACKEND=rocksdb`.

Example tuning templates:

Balanced local profile:

```bash
env \
  GREENMQTT_STORAGE_BACKEND=rocksdb \
  GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES=$((64 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES=$((128 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES=$((16 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS=4 \
  GREENMQTT_ROCKSDB_PARALLELISM=4 \
  ./scripts/run-performance-profile.sh
```

Write-heavy throughput profile:

```bash
env \
  GREENMQTT_STORAGE_BACKEND=rocksdb \
  GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES=$((32 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES=$((256 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES=$((32 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS=6 \
  GREENMQTT_ROCKSDB_PARALLELISM=6 \
  GREENMQTT_ROCKSDB_BYTES_PER_SYNC=$((4 * 1024 * 1024)) \
  ./scripts/run-performance-profile.sh --release
```

Read-heavy lookup profile:

```bash
env \
  GREENMQTT_STORAGE_BACKEND=rocksdb \
  GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES=$((128 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY=12 \
  GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS=true \
  GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO=0.2 \
  ./scripts/run-performance-profile.sh --release
```

For backend comparisons, keep the workload fixed and change one RocksDB variable group at a time:

```bash
env \
  GREENMQTT_COMPARE_BACKENDS=rocksdb \
  GREENMQTT_STORAGE_BACKEND=rocksdb \
  GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES=$((64 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES=$((256 * 1024 * 1024)) \
  GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES=$((32 * 1024 * 1024)) \
  cargo run -p greenmqtt-cli -- compare-bench
```

### Cluster profile

`local` is the operator baseline.  
`ci` is the smaller deterministic gate.

If cluster profile fails:
1. Check residual-state assertions first.
2. Then inspect failover/reconnect behavior.
3. Only enable the flaky soak with `GREENMQTT_CLUSTER_PROFILE_INCLUDE_FLAKY_SOAK=1` for manual diagnosis.

### Storage upgrade baseline

Expected coverage:
- `memory -> durable` migration
- `sled -> rocksdb`
- `rocksdb -> redis`
- `redis -> sled`

If storage upgrade fails:
1. Treat it as a release blocker for backend-switch work.
2. Re-run after confirming local Redis is reachable.

### Compatibility matrix

Expected coverage:
- MQTT 3.1.1 baseline
- MQTT 5 baseline
- TCP/TLS/WS/WSS/QUIC representative flows

If compatibility fails:
1. Identify whether it is protocol-version specific or transport specific.
2. Re-run the single failing step directly with `cargo test -p greenmqtt-broker <test-name> -- --nocapture`.
