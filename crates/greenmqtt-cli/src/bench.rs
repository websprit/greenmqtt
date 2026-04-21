use super::*;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchConfig {
    pub(crate) subscribers: usize,
    pub(crate) publishers: usize,
    pub(crate) messages_per_publisher: usize,
    pub(crate) qos: u8,
    pub(crate) scenario: BenchScenario,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SoakConfig {
    pub(crate) iterations: usize,
    pub(crate) subscribers: usize,
    pub(crate) publishers: usize,
    pub(crate) messages_per_publisher: usize,
    pub(crate) qos: u8,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BenchThresholds {
    pub(crate) min_publishes_per_sec: Option<f64>,
    pub(crate) min_deliveries_per_sec: Option<f64>,
    pub(crate) max_pending_before_drain: Option<usize>,
    pub(crate) max_rss_bytes_after: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SoakThresholds {
    pub(crate) max_final_global_sessions: Option<usize>,
    pub(crate) max_final_routes: Option<usize>,
    pub(crate) max_final_subscriptions: Option<usize>,
    pub(crate) max_final_offline_messages: Option<usize>,
    pub(crate) max_final_inflight_messages: Option<usize>,
    pub(crate) max_final_retained_messages: Option<usize>,
    pub(crate) max_peak_rss_bytes: Option<u64>,
    pub(crate) max_final_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BenchScenario {
    Live,
    OfflineReplay,
    RetainedReplay,
    SharedLive,
}

fn bench_topic_filter(scenario: BenchScenario) -> &'static str {
    match scenario {
        BenchScenario::Live | BenchScenario::OfflineReplay | BenchScenario::SharedLive => {
            "devices/+/state"
        }
        BenchScenario::RetainedReplay => "devices/+/state/+",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct BenchReport {
    pub(crate) storage_backend: String,
    pub(crate) scenario: BenchScenario,
    pub(crate) subscribers: usize,
    pub(crate) publishers: usize,
    pub(crate) messages_per_publisher: usize,
    pub(crate) total_publishes: usize,
    pub(crate) expected_deliveries: usize,
    pub(crate) actual_deliveries: usize,
    pub(crate) matched_routes: usize,
    pub(crate) online_deliveries: usize,
    pub(crate) pending_before_drain: usize,
    pub(crate) global_session_records: usize,
    pub(crate) route_records: usize,
    pub(crate) subscription_records: usize,
    pub(crate) offline_messages_before_drain: usize,
    pub(crate) inflight_messages_before_drain: usize,
    pub(crate) retained_messages: usize,
    pub(crate) rss_bytes_after: u64,
    pub(crate) setup_ms: u128,
    pub(crate) publish_ms: u128,
    pub(crate) drain_ms: u128,
    pub(crate) elapsed_ms: u128,
    pub(crate) publishes_per_sec: f64,
    pub(crate) deliveries_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct SoakReport {
    pub(crate) storage_backend: String,
    pub(crate) iterations: usize,
    pub(crate) subscribers: usize,
    pub(crate) publishers: usize,
    pub(crate) messages_per_publisher: usize,
    pub(crate) total_publishes: usize,
    pub(crate) total_deliveries: usize,
    pub(crate) max_local_online_sessions: usize,
    pub(crate) max_local_pending_deliveries: usize,
    pub(crate) max_global_session_records: usize,
    pub(crate) max_route_records: usize,
    pub(crate) max_subscription_records: usize,
    pub(crate) final_global_session_records: usize,
    pub(crate) final_route_records: usize,
    pub(crate) final_subscription_records: usize,
    pub(crate) final_offline_messages: usize,
    pub(crate) final_inflight_messages: usize,
    pub(crate) final_retained_messages: usize,
    pub(crate) max_rss_bytes: u64,
    pub(crate) final_rss_bytes: u64,
    pub(crate) elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CompareBenchReport {
    pub(crate) reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ProfileBenchReport {
    pub(crate) reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CompareSoakReport {
    pub(crate) reports: Vec<SoakReport>,
}

pub(crate) async fn run_bench<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    node_id: u64,
    config: BenchConfig,
    storage_backend: &str,
) -> anyhow::Result<BenchReport>
where
    A: greenmqtt_plugin_api::AuthProvider,
    C: greenmqtt_plugin_api::AclProvider,
    H: greenmqtt_plugin_api::EventHook,
{
    let setup_started_at = Instant::now();
    let mut publisher_sessions = Vec::with_capacity(config.publishers);
    for index in 0..config.publishers {
        let reply = broker
            .connect(ConnectRequest {
                identity: ClientIdentity {
                    tenant_id: "bench".into(),
                    user_id: format!("pub-{index}"),
                    client_id: format!("pub-{index}"),
                },
                node_id,
                kind: SessionKind::Transient,
                clean_start: true,
                session_expiry_interval_secs: None,
            })
            .await?;
        publisher_sessions.push(reply.session.session_id);
    }

    let mut subscriber_sessions = Vec::with_capacity(config.subscribers);
    if !matches!(config.scenario, BenchScenario::RetainedReplay) {
        for index in 0..config.subscribers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "bench".into(),
                        user_id: format!("sub-{index}"),
                        client_id: format!("sub-{index}"),
                    },
                    node_id,
                    kind: match config.scenario {
                        BenchScenario::Live => SessionKind::Transient,
                        BenchScenario::OfflineReplay => SessionKind::Persistent,
                        BenchScenario::RetainedReplay => SessionKind::Transient,
                        BenchScenario::SharedLive => SessionKind::Transient,
                    },
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            broker
                .subscribe(
                    &reply.session.session_id,
                    bench_topic_filter(config.scenario),
                    config.qos,
                    None,
                    false,
                    false,
                    0,
                    matches!(config.scenario, BenchScenario::SharedLive)
                        .then_some("workers".to_string()),
                )
                .await?;
            subscriber_sessions.push(reply.session.session_id);
            if matches!(config.scenario, BenchScenario::OfflineReplay) {
                broker.disconnect(subscriber_sessions.last().unwrap()).await?;
            }
        }
    }

    let setup_elapsed = setup_started_at.elapsed();
    let publish_started_at = Instant::now();
    let mut matched_routes = 0usize;
    let mut online_deliveries = 0usize;
    for (publisher_index, publisher_session_id) in publisher_sessions.iter().enumerate() {
        for message_index in 0..config.messages_per_publisher {
            let outcome = broker
                .publish(
                    publisher_session_id,
                    PublishRequest {
                        topic: match config.scenario {
                            BenchScenario::Live
                            | BenchScenario::OfflineReplay
                            | BenchScenario::SharedLive => {
                                format!("devices/d{publisher_index}/state")
                            }
                            BenchScenario::RetainedReplay => {
                                format!("devices/d{publisher_index}/state/{message_index}")
                            }
                        },
                        payload: format!("m-{message_index}").into_bytes().into(),
                        qos: config.qos,
                        retain: matches!(config.scenario, BenchScenario::RetainedReplay),
                        properties: PublishProperties::default(),
                    },
                )
                .await?;
            matched_routes += outcome.matched_routes;
            online_deliveries += outcome.online_deliveries;
        }
    }
    let publish_elapsed = publish_started_at.elapsed();
    let stats_before_drain = broker.stats().await?;
    let pending_before_drain = stats_before_drain.local_pending_deliveries;
    let drain_started_at = Instant::now();
    let mut actual_deliveries = 0usize;
    match config.scenario {
        BenchScenario::Live | BenchScenario::SharedLive => {
            for session_id in &subscriber_sessions {
                actual_deliveries += broker.drain_deliveries(session_id).await?.len();
            }
        }
        BenchScenario::OfflineReplay => {
            for index in 0..config.subscribers {
                let resumed = broker
                    .connect(ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "bench".into(),
                            user_id: format!("sub-{index}"),
                            client_id: format!("sub-{index}"),
                        },
                        node_id,
                        kind: SessionKind::Persistent,
                        clean_start: false,
                        session_expiry_interval_secs: None,
                    })
                    .await?;
                actual_deliveries += resumed.offline_messages.len();
                broker.disconnect(&resumed.session.session_id).await?;
            }
        }
        BenchScenario::RetainedReplay => {
            for index in 0..config.subscribers {
                let reply = broker
                    .connect(ConnectRequest {
                        identity: ClientIdentity {
                            tenant_id: "bench".into(),
                            user_id: format!("sub-{index}"),
                            client_id: format!("sub-{index}"),
                        },
                        node_id,
                        kind: SessionKind::Transient,
                        clean_start: true,
                        session_expiry_interval_secs: None,
                    })
                    .await?;
                let retained = broker
                    .subscribe(
                        &reply.session.session_id,
                        bench_topic_filter(config.scenario),
                        config.qos,
                        None,
                        false,
                        false,
                        0,
                        None,
                    )
                    .await?;
                actual_deliveries += retained.len();
                broker.disconnect(&reply.session.session_id).await?;
            }
        }
    }
    let drain_elapsed = drain_started_at.elapsed();
    let elapsed = setup_elapsed + publish_elapsed + drain_elapsed;
    let rss_bytes_after = current_process_rss_bytes();

    let total_publishes = config.publishers * config.messages_per_publisher;
    let expected_deliveries = match config.scenario {
        BenchScenario::SharedLive => total_publishes,
        _ => total_publishes * config.subscribers,
    };
    let elapsed_secs = elapsed.as_secs_f64();
    Ok(BenchReport {
        storage_backend: storage_backend.to_string(),
        scenario: config.scenario,
        subscribers: config.subscribers,
        publishers: config.publishers,
        messages_per_publisher: config.messages_per_publisher,
        total_publishes,
        expected_deliveries,
        actual_deliveries,
        matched_routes,
        online_deliveries,
        pending_before_drain,
        global_session_records: stats_before_drain.global_session_records,
        route_records: stats_before_drain.route_records,
        subscription_records: stats_before_drain.subscription_records,
        offline_messages_before_drain: stats_before_drain.offline_messages,
        inflight_messages_before_drain: stats_before_drain.inflight_messages,
        retained_messages: stats_before_drain.retained_messages,
        rss_bytes_after,
        setup_ms: setup_elapsed.as_millis(),
        publish_ms: publish_elapsed.as_millis(),
        drain_ms: drain_elapsed.as_millis(),
        elapsed_ms: elapsed.as_millis(),
        publishes_per_sec: if elapsed_secs > 0.0 {
            total_publishes as f64 / elapsed_secs
        } else {
            total_publishes as f64
        },
        deliveries_per_sec: if elapsed_secs > 0.0 {
            actual_deliveries as f64 / elapsed_secs
        } else {
            actual_deliveries as f64
        },
    })
}

pub(crate) async fn run_compare_bench(
    node_id: u64,
    config: BenchConfig,
    backends: &[String],
    redis_url: &str,
) -> anyhow::Result<CompareBenchReport> {
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    let mut reports = Vec::with_capacity(backends.len());
    for backend in backends {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_bench(broker, node_id, config, backend).await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(CompareBenchReport { reports })
}

pub(crate) async fn run_compare_soak(
    node_id: u64,
    config: SoakConfig,
    backends: &[String],
    redis_url: &str,
) -> anyhow::Result<CompareSoakReport> {
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    let mut reports = Vec::with_capacity(backends.len());
    for backend in backends {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_soak(broker, node_id, config, backend).await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(CompareSoakReport { reports })
}

pub(crate) async fn run_profile_bench(
    node_id: u64,
    config: BenchConfig,
    scenarios: &[BenchScenario],
    backend: &str,
    redis_url: &str,
) -> anyhow::Result<ProfileBenchReport> {
    anyhow::ensure!(
        !scenarios.is_empty(),
        "GREENMQTT_PROFILE_SCENARIOS must include at least one scenario"
    );
    let mut reports = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let (broker, cleanup_dir) = compare_bench_broker(node_id, backend, redis_url).await?;
        let report = run_bench(
            broker,
            node_id,
            BenchConfig {
                scenario: *scenario,
                ..config
            },
            backend,
        )
        .await?;
        if let Some(path) = cleanup_dir {
            let _ = fs::remove_dir_all(path);
        }
        reports.push(report);
    }
    Ok(ProfileBenchReport { reports })
}

pub(crate) async fn run_soak<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    node_id: u64,
    config: SoakConfig,
    storage_backend: &str,
) -> anyhow::Result<SoakReport>
where
    A: greenmqtt_plugin_api::AuthProvider,
    C: greenmqtt_plugin_api::AclProvider,
    H: greenmqtt_plugin_api::EventHook,
{
    let started_at = Instant::now();
    let mut total_publishes = 0usize;
    let mut total_deliveries = 0usize;
    let mut max_local_online_sessions = 0usize;
    let mut max_local_pending_deliveries = 0usize;
    let mut max_global_session_records = 0usize;
    let mut max_route_records = 0usize;
    let mut max_subscription_records = 0usize;
    let mut max_rss_bytes = current_process_rss_bytes();

    for iteration in 0..config.iterations {
        let mut subscriber_sessions = Vec::with_capacity(config.subscribers);
        for index in 0..config.subscribers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "soak".into(),
                        user_id: format!("sub-{iteration}-{index}"),
                        client_id: format!("sub-{iteration}-{index}"),
                    },
                    node_id,
                    kind: SessionKind::Transient,
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            broker
                .subscribe(
                    &reply.session.session_id,
                    "devices/+/state",
                    config.qos,
                    None,
                    false,
                    false,
                    0,
                    None,
                )
                .await?;
            subscriber_sessions.push(reply.session.session_id);
        }

        let mut publisher_sessions = Vec::with_capacity(config.publishers);
        for index in 0..config.publishers {
            let reply = broker
                .connect(ConnectRequest {
                    identity: ClientIdentity {
                        tenant_id: "soak".into(),
                        user_id: format!("pub-{iteration}-{index}"),
                        client_id: format!("pub-{iteration}-{index}"),
                    },
                    node_id,
                    kind: SessionKind::Transient,
                    clean_start: true,
                    session_expiry_interval_secs: None,
                })
                .await?;
            publisher_sessions.push(reply.session.session_id);
        }

        for (publisher_index, publisher_session_id) in publisher_sessions.iter().enumerate() {
            for message_index in 0..config.messages_per_publisher {
                broker
                    .publish(
                        publisher_session_id,
                        PublishRequest {
                            topic: format!("devices/d{publisher_index}/state"),
                            payload: format!("soak-{iteration}-{message_index}")
                                .into_bytes()
                                .into(),
                            qos: config.qos,
                            retain: false,
                            properties: PublishProperties::default(),
                        },
                    )
                    .await?;
                total_publishes += 1;
            }
        }

        let stats_during = broker.stats().await?;
        max_local_online_sessions =
            max_local_online_sessions.max(stats_during.local_online_sessions);
        max_local_pending_deliveries =
            max_local_pending_deliveries.max(stats_during.local_pending_deliveries);
        max_global_session_records =
            max_global_session_records.max(stats_during.global_session_records);
        max_route_records = max_route_records.max(stats_during.route_records);
        max_subscription_records = max_subscription_records.max(stats_during.subscription_records);
        max_rss_bytes = max_rss_bytes.max(current_process_rss_bytes());

        for session_id in &subscriber_sessions {
            total_deliveries += broker.drain_deliveries(session_id).await?.len();
        }

        for session_id in subscriber_sessions.iter().chain(publisher_sessions.iter()) {
            broker.disconnect(session_id).await?;
        }
    }

    let final_stats = broker.stats().await?;
    let final_rss_bytes = current_process_rss_bytes();
    Ok(SoakReport {
        storage_backend: storage_backend.to_string(),
        iterations: config.iterations,
        subscribers: config.subscribers,
        publishers: config.publishers,
        messages_per_publisher: config.messages_per_publisher,
        total_publishes,
        total_deliveries,
        max_local_online_sessions,
        max_local_pending_deliveries,
        max_global_session_records,
        max_route_records,
        max_subscription_records,
        final_global_session_records: final_stats.global_session_records,
        final_route_records: final_stats.route_records,
        final_subscription_records: final_stats.subscription_records,
        final_offline_messages: final_stats.offline_messages,
        final_inflight_messages: final_stats.inflight_messages,
        final_retained_messages: final_stats.retained_messages,
        max_rss_bytes,
        final_rss_bytes,
        elapsed_ms: started_at.elapsed().as_millis(),
    })
}

pub(crate) fn storage_backend_label(
    state_endpoint: Option<&str>,
    data_dir: Option<&str>,
    storage_backend: &str,
) -> String {
    if state_endpoint.is_some() {
        "remote_grpc".to_string()
    } else if storage_backend == "redis" {
        "redis".to_string()
    } else if data_dir.is_some() {
        storage_backend.to_string()
    } else {
        "memory".to_string()
    }
}

fn current_process_rss_bytes() -> u64 {
    let Ok(pid) = get_current_pid() else {
        return 0;
    };
    let system = System::new_all();
    system
        .process(pid)
        .map(|process| process.memory())
        .unwrap_or(0)
}

pub(crate) fn bench_thresholds_from_env() -> BenchThresholds {
    BenchThresholds {
        min_publishes_per_sec: std::env::var("GREENMQTT_BENCH_MIN_PUBLISHES_PER_SEC")
            .ok()
            .and_then(|value| value.parse().ok()),
        min_deliveries_per_sec: std::env::var("GREENMQTT_BENCH_MIN_DELIVERIES_PER_SEC")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_pending_before_drain: std::env::var("GREENMQTT_BENCH_MAX_PENDING_BEFORE_DRAIN")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_rss_bytes_after: std::env::var("GREENMQTT_BENCH_MAX_RSS_BYTES_AFTER")
            .ok()
            .and_then(|value| value.parse().ok()),
    }
}

pub(crate) fn soak_thresholds_from_env() -> SoakThresholds {
    SoakThresholds {
        max_final_global_sessions: std::env::var("GREENMQTT_SOAK_MAX_FINAL_GLOBAL_SESSIONS")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_routes: std::env::var("GREENMQTT_SOAK_MAX_FINAL_ROUTES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_subscriptions: std::env::var("GREENMQTT_SOAK_MAX_FINAL_SUBSCRIPTIONS")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_offline_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_OFFLINE_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_inflight_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_INFLIGHT_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_retained_messages: std::env::var("GREENMQTT_SOAK_MAX_FINAL_RETAINED_MESSAGES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_peak_rss_bytes: std::env::var("GREENMQTT_SOAK_MAX_PEAK_RSS_BYTES")
            .ok()
            .and_then(|value| value.parse().ok()),
        max_final_rss_bytes: std::env::var("GREENMQTT_SOAK_MAX_FINAL_RSS_BYTES")
            .ok()
            .and_then(|value| value.parse().ok()),
    }
}

pub(crate) fn parse_bench_scenario(value: &str) -> anyhow::Result<BenchScenario> {
    match value {
        "live" => Ok(BenchScenario::Live),
        "offline_replay" => Ok(BenchScenario::OfflineReplay),
        "retained_replay" => Ok(BenchScenario::RetainedReplay),
        "shared_live" => Ok(BenchScenario::SharedLive),
        other => anyhow::bail!("unsupported GREENMQTT_BENCH_SCENARIO: {other}"),
    }
}

pub(crate) fn parse_bench_scenarios(value: &str) -> anyhow::Result<Vec<BenchScenario>> {
    let scenarios = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse_bench_scenario)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(
        !scenarios.is_empty(),
        "GREENMQTT_PROFILE_SCENARIOS must include at least one scenario"
    );
    Ok(scenarios)
}

pub(crate) fn parse_compare_backends(value: &str) -> anyhow::Result<Vec<String>> {
    let mut backends = Vec::new();
    for backend in value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match backend {
            "memory" | "sled" | "rocksdb" | "redis" => backends.push(backend.to_string()),
            other => anyhow::bail!("unsupported GREENMQTT_COMPARE_BACKENDS value: {other}"),
        }
    }
    anyhow::ensure!(
        !backends.is_empty(),
        "GREENMQTT_COMPARE_BACKENDS must include at least one backend"
    );
    Ok(backends)
}

pub(crate) fn print_bench_report(
    report: &BenchReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!(
                "greenmqtt bench: storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, global_sessions={}, routes={}, subscriptions={}, offline_messages={}, inflight_messages={}, retained_messages={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                report.storage_backend,
                report.scenario,
                report.total_publishes,
                report.expected_deliveries,
                report.actual_deliveries,
                report.matched_routes,
                report.online_deliveries,
                report.pending_before_drain,
                report.global_session_records,
                report.route_records,
                report.subscription_records,
                report.offline_messages_before_drain,
                report.inflight_messages_before_drain,
                report.retained_messages,
                report.rss_bytes_after,
                report.setup_ms,
                report.publish_ms,
                report.drain_ms,
                report.elapsed_ms,
                report.publishes_per_sec,
                report.deliveries_per_sec,
            );
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

pub(crate) fn print_compare_bench_report(
    report: &CompareBenchReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt compare-bench:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                    item.storage_backend,
                    item.scenario,
                    item.total_publishes,
                    item.expected_deliveries,
                    item.actual_deliveries,
                    item.matched_routes,
                    item.online_deliveries,
                    item.pending_before_drain,
                    item.rss_bytes_after,
                    item.setup_ms,
                    item.publish_ms,
                    item.drain_ms,
                    item.elapsed_ms,
                    item.publishes_per_sec,
                    item.deliveries_per_sec,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

pub(crate) fn print_profile_bench_report(
    report: &ProfileBenchReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt profile-bench:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, scenario={:?}, publishes={}, expected_deliveries={}, actual_deliveries={}, matched_routes={}, online_deliveries={}, pending_before_drain={}, rss_bytes_after={}, setup_ms={}, publish_ms={}, drain_ms={}, elapsed_ms={}, publishes_per_sec={:.2}, deliveries_per_sec={:.2}",
                    item.storage_backend,
                    item.scenario,
                    item.total_publishes,
                    item.expected_deliveries,
                    item.actual_deliveries,
                    item.matched_routes,
                    item.online_deliveries,
                    item.pending_before_drain,
                    item.rss_bytes_after,
                    item.setup_ms,
                    item.publish_ms,
                    item.drain_ms,
                    item.elapsed_ms,
                    item.publishes_per_sec,
                    item.deliveries_per_sec,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

pub(crate) fn print_compare_soak_report(
    report: &CompareSoakReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!("greenmqtt compare-soak:");
            for item in &report.reports {
                println!(
                    "  storage_backend={}, iterations={}, publishes={}, deliveries={}, max_local_online_sessions={}, max_local_pending_deliveries={}, max_global_sessions={}, max_routes={}, max_subscriptions={}, final_global_sessions={}, final_routes={}, final_subscriptions={}, final_offline_messages={}, final_inflight_messages={}, final_retained_messages={}, max_rss_bytes={}, final_rss_bytes={}, elapsed_ms={}",
                    item.storage_backend,
                    item.iterations,
                    item.total_publishes,
                    item.total_deliveries,
                    item.max_local_online_sessions,
                    item.max_local_pending_deliveries,
                    item.max_global_session_records,
                    item.max_route_records,
                    item.max_subscription_records,
                    item.final_global_session_records,
                    item.final_route_records,
                    item.final_subscription_records,
                    item.final_offline_messages,
                    item.final_inflight_messages,
                    item.final_retained_messages,
                    item.max_rss_bytes,
                    item.final_rss_bytes,
                    item.elapsed_ms,
                );
            }
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

pub(crate) fn print_soak_report(
    report: &SoakReport,
    output_mode: OutputMode,
) -> anyhow::Result<()> {
    match output_mode {
        OutputMode::Text => {
            println!(
                "greenmqtt soak: storage_backend={}, iterations={}, publishes={}, deliveries={}, max_local_online_sessions={}, max_local_pending_deliveries={}, max_global_sessions={}, max_routes={}, max_subscriptions={}, final_global_sessions={}, final_routes={}, final_subscriptions={}, final_offline_messages={}, final_inflight_messages={}, final_retained_messages={}, max_rss_bytes={}, final_rss_bytes={}, elapsed_ms={}",
                report.storage_backend,
                report.iterations,
                report.total_publishes,
                report.total_deliveries,
                report.max_local_online_sessions,
                report.max_local_pending_deliveries,
                report.max_global_session_records,
                report.max_route_records,
                report.max_subscription_records,
                report.final_global_session_records,
                report.final_route_records,
                report.final_subscription_records,
                report.final_offline_messages,
                report.final_inflight_messages,
                report.final_retained_messages,
                report.max_rss_bytes,
                report.final_rss_bytes,
                report.elapsed_ms,
            );
        }
        OutputMode::Json => {
            println!("{}", serde_json::to_string(report)?);
        }
    }
    Ok(())
}

pub(crate) fn validate_bench_report(
    report: &BenchReport,
    thresholds: BenchThresholds,
) -> anyhow::Result<()> {
    if let Some(min) = thresholds.min_publishes_per_sec {
        anyhow::ensure!(
            report.publishes_per_sec >= min,
            "bench publishes_per_sec {:.2} below threshold {:.2}",
            report.publishes_per_sec,
            min
        );
    }
    if let Some(min) = thresholds.min_deliveries_per_sec {
        anyhow::ensure!(
            report.deliveries_per_sec >= min,
            "bench deliveries_per_sec {:.2} below threshold {:.2}",
            report.deliveries_per_sec,
            min
        );
    }
    if let Some(max) = thresholds.max_pending_before_drain {
        anyhow::ensure!(
            report.pending_before_drain <= max,
            "bench pending_before_drain {} exceeds threshold {}",
            report.pending_before_drain,
            max
        );
    }
    if let Some(max) = thresholds.max_rss_bytes_after {
        anyhow::ensure!(
            report.rss_bytes_after <= max,
            "bench rss_bytes_after {} exceeds threshold {}",
            report.rss_bytes_after,
            max
        );
    }
    Ok(())
}

pub(crate) fn validate_soak_report(
    report: &SoakReport,
    thresholds: SoakThresholds,
) -> anyhow::Result<()> {
    if let Some(max) = thresholds.max_final_global_sessions {
        anyhow::ensure!(
            report.final_global_session_records <= max,
            "soak final_global_sessions {} exceeds threshold {}",
            report.final_global_session_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_routes {
        anyhow::ensure!(
            report.final_route_records <= max,
            "soak final_routes {} exceeds threshold {}",
            report.final_route_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_subscriptions {
        anyhow::ensure!(
            report.final_subscription_records <= max,
            "soak final_subscriptions {} exceeds threshold {}",
            report.final_subscription_records,
            max
        );
    }
    if let Some(max) = thresholds.max_final_offline_messages {
        anyhow::ensure!(
            report.final_offline_messages <= max,
            "soak final_offline_messages {} exceeds threshold {}",
            report.final_offline_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_final_inflight_messages {
        anyhow::ensure!(
            report.final_inflight_messages <= max,
            "soak final_inflight_messages {} exceeds threshold {}",
            report.final_inflight_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_final_retained_messages {
        anyhow::ensure!(
            report.final_retained_messages <= max,
            "soak final_retained_messages {} exceeds threshold {}",
            report.final_retained_messages,
            max
        );
    }
    if let Some(max) = thresholds.max_peak_rss_bytes {
        anyhow::ensure!(
            report.max_rss_bytes <= max,
            "soak max_rss_bytes {} exceeds threshold {}",
            report.max_rss_bytes,
            max
        );
    }
    if let Some(max) = thresholds.max_final_rss_bytes {
        anyhow::ensure!(
            report.final_rss_bytes <= max,
            "soak final_rss_bytes {} exceeds threshold {}",
            report.final_rss_bytes,
            max
        );
    }
    Ok(())
}
