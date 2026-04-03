.PHONY: check test run bench compare-bench profile-bench soak compare-soak ci-bench ci-bench-offline ci-bench-retained ci-bench-shared ci-profile-bench ci-profile-bench-redis ci-compare-bench ci-compare-bench-redis ci-durable-bench ci-durable-soak ci-soak ci-compare-soak ci-compare-soak-redis ci-cluster-bench ci-cluster-bench-shared ci-cluster-bench-offline ci-cluster-dynamic-peers ci-cluster-failover-soak ci

check:
	cargo check

test:
	cargo test -q

run:
	cargo run -p greenmqtt-cli

bench:
	cargo run -p greenmqtt-cli -- bench

compare-bench:
	cargo run -p greenmqtt-cli -- compare-bench

profile-bench:
	cargo run -p greenmqtt-cli -- profile-bench

soak:
	cargo run -p greenmqtt-cli -- soak

compare-soak:
	cargo run -p greenmqtt-cli -- compare-soak

ci-bench:
	GREENMQTT_BENCH_SUBSCRIBERS=20 \
	GREENMQTT_BENCH_PUBLISHERS=4 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=25 \
	GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC=1000 \
	GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC=10000 \
	GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN=2000 \
	GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER=134217728 \
	cargo run -q -p greenmqtt-cli -- bench

ci-bench-offline:
	GREENMQTT_BENCH_SCENARIO=offline_replay \
	GREENMQTT_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_BENCH_PUBLISHERS=2 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=10 \
	GREENMQTT_BENCH_QOS=1 \
	GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC=200 \
	GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC=1000 \
	GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN=0 \
	GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER=134217728 \
	cargo run -q -p greenmqtt-cli -- bench

ci-bench-retained:
	GREENMQTT_BENCH_SCENARIO=retained_replay \
	GREENMQTT_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_BENCH_PUBLISHERS=2 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=10 \
	GREENMQTT_BENCH_QOS=1 \
	GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC=200 \
	GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC=1000 \
	GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN=0 \
	GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER=134217728 \
	cargo run -q -p greenmqtt-cli -- bench

ci-bench-shared:
	GREENMQTT_BENCH_SCENARIO=shared_live \
	GREENMQTT_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_BENCH_PUBLISHERS=2 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=10 \
	GREENMQTT_BENCH_QOS=1 \
	GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC=200 \
	GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC=200 \
	GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN=200 \
	GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER=134217728 \
	cargo run -q -p greenmqtt-cli -- bench

ci-profile-bench:
	GREENMQTT_PROFILE_SCENARIOS=live,offline_replay,retained_replay,shared_live \
	GREENMQTT_STORAGE_BACKEND=memory \
	GREENMQTT_BENCH_SUBSCRIBERS=5 \
	GREENMQTT_BENCH_PUBLISHERS=2 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=5 \
	GREENMQTT_BENCH_QOS=1 \
	cargo run -q -p greenmqtt-cli -- profile-bench

ci-profile-bench-redis:
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::profile_bench_report_runs_with_redis_backend -- --exact

ci-compare-bench:
	GREENMQTT_COMPARE_BACKENDS=memory,sled,rocksdb \
	GREENMQTT_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_BENCH_PUBLISHERS=2 \
	GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER=10 \
	GREENMQTT_BENCH_QOS=1 \
	cargo run -q -p greenmqtt-cli -- compare-bench

ci-compare-bench-redis:
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::compare_bench_report_runs_with_memory_and_redis_backends -- --exact

ci-durable-bench:
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_runs_with_sled_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_runs_with_rocksdb_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_runs_with_redis_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_offline_replay_runs_with_sled_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_offline_replay_runs_with_redis_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_retained_replay_runs_with_sled_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_retained_replay_runs_with_redis_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_shared_live_runs_with_sled_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::bench_report_shared_live_runs_with_redis_durable_backend -- --exact

ci-durable-soak:
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::soak_report_runs_with_sled_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::soak_report_runs_with_rocksdb_durable_backend -- --exact
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::soak_report_runs_with_redis_durable_backend -- --exact

ci-soak:
	GREENMQTT_SOAK_ITERATIONS=20 \
	GREENMQTT_SOAK_SUBSCRIBERS=10 \
	GREENMQTT_SOAK_PUBLISHERS=4 \
	GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER=10 \
	GREENMQTT_SOAK_MAX_FINAL_GLOBAL_SESSIONS=0 \
	GREENMQTT_SOAK_MAX_FINAL_ROUTES=0 \
	GREENMQTT_SOAK_MAX_FINAL_SUBSCRIPTIONS=0 \
	GREENMQTT_SOAK_MAX_FINAL_OFFLINE_MESSAGES=0 \
	GREENMQTT_SOAK_MAX_FINAL_INFLIGHT_MESSAGES=0 \
	GREENMQTT_SOAK_MAX_FINAL_RETAINED_MESSAGES=0 \
	GREENMQTT_SOAK_MAX_PEAK_RSS_BYTES=134217728 \
	GREENMQTT_SOAK_MAX_FINAL_RSS_BYTES=134217728 \
	cargo run -q -p greenmqtt-cli -- soak

ci-compare-soak:
	GREENMQTT_COMPARE_BACKENDS=memory,sled,rocksdb \
	GREENMQTT_SOAK_ITERATIONS=5 \
	GREENMQTT_SOAK_SUBSCRIBERS=5 \
	GREENMQTT_SOAK_PUBLISHERS=2 \
	GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER=5 \
	GREENMQTT_SOAK_QOS=1 \
	cargo run -q -p greenmqtt-cli -- compare-soak

ci-compare-soak-redis:
	cargo test -q -p greenmqtt-cli --bin greenmqtt-cli tests::compare_soak_report_runs_with_memory_and_redis_backends -- --exact

ci-cluster-bench:
	GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_CLUSTER_BENCH_MESSAGES=20 \
	GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC=500 \
	GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES=134217728 \
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_cross_node_bench_meets_delivery_thresholds -- --exact

ci-cluster-bench-shared:
	GREENMQTT_CLUSTER_BENCH_SCENARIO=shared_live \
	GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_CLUSTER_BENCH_MESSAGES=20 \
	GREENMQTT_CLUSTER_BENCH_QOS=1 \
	GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC=10 \
	GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES=134217728 \
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_cross_node_shared_bench_meets_delivery_thresholds -- --exact

ci-cluster-bench-offline:
	GREENMQTT_CLUSTER_BENCH_SCENARIO=offline_replay \
	GREENMQTT_CLUSTER_BENCH_SUBSCRIBERS=10 \
	GREENMQTT_CLUSTER_BENCH_MESSAGES=20 \
	GREENMQTT_CLUSTER_BENCH_QOS=1 \
	GREENMQTT_CLUSTER_BENCH_MIN_DELIVERIES_PER_SEC=50 \
	GREENMQTT_CLUSTER_BENCH_MAX_NODE_RSS_BYTES=134217728 \
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_cross_node_offline_replay_bench_meets_delivery_thresholds -- --exact

ci-cluster-dynamic-peers:
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_dynamic_peer_upsert_enables_cross_node_delivery -- --exact
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_dynamic_peer_delete_falls_back_to_offline_replay -- --exact
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_dynamic_peer_reupsert_restores_live_forwarding -- --exact
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_dynamic_peer_restores_after_restart_from_audit_log -- --exact
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_sessiondict_reassign_moves_offline_replay_to_new_node -- --exact

ci-cluster-failover-soak:
	GREENMQTT_CLUSTER_FAILOVER_SOAK_ITERATIONS=3 \
	GREENMQTT_REDIS_SERVER_BIN=$${GREENMQTT_REDIS_SERVER_BIN:-redis-server} \
	cargo test -q -p greenmqtt-cli --test cluster_redis cluster_redis_repeated_failover_replays_without_offline_leak -- --exact

ci: test ci-bench ci-bench-offline ci-bench-retained ci-bench-shared ci-profile-bench ci-profile-bench-redis ci-compare-bench ci-compare-bench-redis ci-durable-bench ci-durable-soak ci-soak ci-compare-soak ci-compare-soak-redis ci-cluster-bench ci-cluster-bench-shared ci-cluster-bench-offline ci-cluster-dynamic-peers ci-cluster-failover-soak
