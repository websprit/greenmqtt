use async_trait::async_trait;
use greenmqtt_broker::mqtt::{
    serve_quic_with_profile, serve_tcp_with_profile, serve_tls_with_profile, serve_ws_with_profile,
    serve_wss_with_profile,
};
use greenmqtt_broker::{BrokerConfig, BrokerRuntime};
use greenmqtt_core::{
    ClientIdentity, ClusterMembershipRegistry, ClusterNodeLifecycle, ClusterNodeMembership,
    ConnectRequest, Lifecycle, MetadataRegistry, PublishProperties, PublishRequest, RangeBoundary,
    RangeReplica, ReplicaRole, ReplicaSyncState, ReplicatedRangeDescriptor, ServiceEndpoint,
    ServiceKind, ServiceShardKey, ServiceShardKind, ServiceShardLifecycle, SessionKind,
    ShardControlRegistry,
};
use greenmqtt_dist::{
    DistBalanceAction, DistBalancePolicy, DistHandle, DistMaintenanceWorker, DistRouter,
    DistTenantStats, PersistentDistHandle, ReplicatedDistHandle, ThresholdDistBalancePolicy,
};
use greenmqtt_http_api::HttpApi;
use greenmqtt_inbox::{
    DelayedLwtPublish, DelayedLwtSink, InboxBalanceAction, InboxBalancePolicy,
    InboxDelayTaskRunner, InboxDelayedTask, InboxHandle, InboxMaintenanceWorker, InboxService,
    InboxServiceDelayHandler, InboxTenantStats, PersistentInboxHandle, ReplicatedInboxHandle,
    TenantInboxGcRunner, ThresholdInboxBalancePolicy,
};
use greenmqtt_kv_client::{KvRangeExecutor, KvRangeRouter, LeaderRoutedKvRangeExecutor};
use greenmqtt_kv_engine::{
    KvEngine, KvMutation, KvRangeBootstrap, KvRangeCheckpoint, KvRangeSnapshot, RocksDbKvEngine,
};
use greenmqtt_kv_raft::{MemoryRaftNode, RaftNode};
use greenmqtt_kv_server::{HostedRange, KvRangeHost, MemoryKvRangeHost, RangeLifecycleManager, ReplicaRuntime};
use greenmqtt_plugin_api::{
    current_listener_profile, AclAction, AclDecision, AclProvider, AclRule, AuthProvider,
    BridgeEventHook, BridgeRule, ConfiguredAcl, ConfiguredAuth, ConfiguredEventHook, EventHook,
    HookTarget, HttpAclConfig, HttpAclProvider, HttpAuthConfig, HttpAuthProvider, IdentityMatcher,
    StaticAclProvider, StaticAuthProvider, StaticEnhancedAuthProvider, TopicRewriteEventHook,
    TopicRewriteRule, WebHookConfig, WebHookEventHook, BRIDGE_CLIENT_ID_PREFIX,
};
use greenmqtt_retain::{
    PersistentRetainHandle, ReplicatedRetainHandle, RetainBalanceAction, RetainBalancePolicy,
    RetainHandle, RetainMaintenanceWorker, RetainService, RetainTenantStats,
    ThresholdRetainBalancePolicy,
};
use greenmqtt_rpc::{
    DistGrpcClient, InboxGrpcClient, KvRangeGrpcClient, KvRangeGrpcExecutorFactory,
    MetadataGrpcClient, MetadataPlaneLayout, PersistentMetadataRegistry,
    NoopReplicaTransport, RangeControlGrpcClient, ReplicatedMetadataRegistry,
    RetainGrpcClient, RpcRuntime, SessionDictGrpcClient, StaticPeerForwarder,
    StaticServiceEndpointRegistry,
};
use greenmqtt_sessiondict::{
    PersistentSessionDictHandle, ReplicatedSessionDictHandle, SessionDictHandle, SessionDirectory,
};
use greenmqtt_storage::{
    RedisInboxStore, RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore, SledInboxStore, SledInflightStore, SledRetainStore, SledRouteStore,
    SledSessionStore, SledSubscriptionStore,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::time::Instant;
use sysinfo::{get_current_pid, System};
use tokio::task::JoinSet;

type AppBroker = Arc<BrokerRuntime<PortMappedAuth, PortMappedAcl, PortMappedEventHook>>;
type AppStateServices = (
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
);
type CompareStateServices = (
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
    Option<PathBuf>,
);

struct LocalRangeRuntimeStack {
    sessiondict: Arc<dyn SessionDirectory>,
    dist: Arc<dyn DistRouter>,
    inbox: Arc<dyn InboxService>,
    retain: Arc<dyn RetainService>,
    range_host: Arc<dyn KvRangeHost>,
    range_runtime: Arc<ReplicaRuntime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenerSpec {
    bind: SocketAddr,
    profile: String,
}

#[derive(Clone)]
struct PortMappedAuth {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredAuth>>,
}

#[derive(Clone)]
struct PortMappedAcl {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredAcl>>,
}

#[derive(Clone)]
struct PortMappedEventHook {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredEventHook>>,
}

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    qos: u8,
    scenario: BenchScenario,
}

#[derive(Debug, Clone, Copy)]
struct SoakConfig {
    iterations: usize,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    qos: u8,
}

#[derive(Debug, Clone, Copy, Default)]
struct BenchThresholds {
    min_publishes_per_sec: Option<f64>,
    min_deliveries_per_sec: Option<f64>,
    max_pending_before_drain: Option<usize>,
    max_rss_bytes_after: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct SoakThresholds {
    max_final_global_sessions: Option<usize>,
    max_final_routes: Option<usize>,
    max_final_subscriptions: Option<usize>,
    max_final_offline_messages: Option<usize>,
    max_final_inflight_messages: Option<usize>,
    max_final_retained_messages: Option<usize>,
    max_peak_rss_bytes: Option<u64>,
    max_final_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Text,
    Json,
}

impl PortMappedAuth {
    fn new(default_profile: impl Into<String>, profiles: HashMap<String, ConfiguredAuth>) -> Self {
        Self {
            default_profile: default_profile.into(),
            profiles: Arc::new(profiles),
        }
    }

    fn profile(&self) -> &ConfiguredAuth {
        let active = current_listener_profile().unwrap_or_else(|| self.default_profile.clone());
        self.profiles
            .get(&active)
            .or_else(|| self.profiles.get(&self.default_profile))
            .expect("listener auth profile missing")
    }
}

impl PortMappedAcl {
    fn new(default_profile: impl Into<String>, profiles: HashMap<String, ConfiguredAcl>) -> Self {
        Self {
            default_profile: default_profile.into(),
            profiles: Arc::new(profiles),
        }
    }

    fn profile(&self) -> &ConfiguredAcl {
        let active = current_listener_profile().unwrap_or_else(|| self.default_profile.clone());
        self.profiles
            .get(&active)
            .or_else(|| self.profiles.get(&self.default_profile))
            .expect("listener acl profile missing")
    }
}

impl PortMappedEventHook {
    fn new(
        default_profile: impl Into<String>,
        profiles: HashMap<String, ConfiguredEventHook>,
    ) -> Self {
        Self {
            default_profile: default_profile.into(),
            profiles: Arc::new(profiles),
        }
    }

    fn profile(&self) -> &ConfiguredEventHook {
        let active = current_listener_profile().unwrap_or_else(|| self.default_profile.clone());
        self.profiles
            .get(&active)
            .or_else(|| self.profiles.get(&self.default_profile))
            .expect("listener hook profile missing")
    }
}

#[async_trait]
impl AuthProvider for PortMappedAuth {
    async fn authenticate(&self, identity: &ClientIdentity) -> anyhow::Result<bool> {
        self.profile().authenticate(identity).await
    }

    async fn begin_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<greenmqtt_plugin_api::EnhancedAuthResult> {
        self.profile()
            .begin_enhanced_auth(identity, method, auth_data)
            .await
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<greenmqtt_plugin_api::EnhancedAuthResult> {
        self.profile()
            .continue_enhanced_auth(identity, method, auth_data)
            .await
    }
}

#[async_trait]
impl AclProvider for PortMappedAcl {
    async fn can_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<bool> {
        self.profile().can_subscribe(identity, subscription).await
    }

    async fn can_publish(&self, identity: &ClientIdentity, topic: &str) -> anyhow::Result<bool> {
        self.profile().can_publish(identity, topic).await
    }
}

#[async_trait]
impl EventHook for PortMappedEventHook {
    async fn rewrite_publish(
        &self,
        identity: &ClientIdentity,
        request: &greenmqtt_core::PublishRequest,
    ) -> anyhow::Result<greenmqtt_core::PublishRequest> {
        self.profile().rewrite_publish(identity, request).await
    }

    async fn on_connect(&self, session: &greenmqtt_core::SessionRecord) -> anyhow::Result<()> {
        self.profile().on_connect(session).await
    }

    async fn on_disconnect(&self, session: &greenmqtt_core::SessionRecord) -> anyhow::Result<()> {
        self.profile().on_disconnect(session).await
    }

    async fn on_subscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<()> {
        self.profile().on_subscribe(identity, subscription).await
    }

    async fn on_unsubscribe(
        &self,
        identity: &ClientIdentity,
        subscription: &greenmqtt_core::Subscription,
    ) -> anyhow::Result<()> {
        self.profile().on_unsubscribe(identity, subscription).await
    }

    async fn on_offline_enqueue(
        &self,
        message: &greenmqtt_core::OfflineMessage,
    ) -> anyhow::Result<()> {
        self.profile().on_offline_enqueue(message).await
    }

    async fn on_retain_write(
        &self,
        message: &greenmqtt_core::RetainedMessage,
    ) -> anyhow::Result<()> {
        self.profile().on_retain_write(message).await
    }

    async fn on_publish(
        &self,
        identity: &ClientIdentity,
        request: &greenmqtt_core::PublishRequest,
        outcome: &greenmqtt_core::PublishOutcome,
    ) -> anyhow::Result<()> {
        self.profile().on_publish(identity, request, outcome).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BenchScenario {
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
struct BenchReport {
    storage_backend: String,
    scenario: BenchScenario,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    total_publishes: usize,
    expected_deliveries: usize,
    actual_deliveries: usize,
    matched_routes: usize,
    online_deliveries: usize,
    pending_before_drain: usize,
    global_session_records: usize,
    route_records: usize,
    subscription_records: usize,
    offline_messages_before_drain: usize,
    inflight_messages_before_drain: usize,
    retained_messages: usize,
    rss_bytes_after: u64,
    setup_ms: u128,
    publish_ms: u128,
    drain_ms: u128,
    elapsed_ms: u128,
    publishes_per_sec: f64,
    deliveries_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SoakReport {
    storage_backend: String,
    iterations: usize,
    subscribers: usize,
    publishers: usize,
    messages_per_publisher: usize,
    total_publishes: usize,
    total_deliveries: usize,
    max_local_online_sessions: usize,
    max_local_pending_deliveries: usize,
    max_global_session_records: usize,
    max_route_records: usize,
    max_subscription_records: usize,
    final_global_session_records: usize,
    final_route_records: usize,
    final_subscription_records: usize,
    final_offline_messages: usize,
    final_inflight_messages: usize,
    final_retained_messages: usize,
    max_rss_bytes: u64,
    final_rss_bytes: u64,
    elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompareBenchReport {
    reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ProfileBenchReport {
    reports: Vec<BenchReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompareSoakReport {
    reports: Vec<SoakReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ShardActionEnvelope {
    previous: Option<greenmqtt_core::ServiceShardAssignment>,
    current: Option<greenmqtt_core::ServiceShardAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RangeCliCommand {
    Bootstrap {
        descriptor: ReplicatedRangeDescriptor,
    },
    Split {
        range_id: String,
        split_key: Vec<u8>,
    },
    Merge {
        left_range_id: String,
        right_range_id: String,
    },
    Drain {
        range_id: String,
    },
    Retire {
        range_id: String,
    },
    Recover {
        range_id: String,
        new_leader_node_id: u64,
    },
    ChangeReplicas {
        range_id: String,
        voters: Vec<u64>,
        learners: Vec<u64>,
    },
    TransferLeadership {
        range_id: String,
        target_node_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RangeCliReply {
    operation: String,
    ok: bool,
    status: String,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    left_range_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    right_range_id: Option<String>,
}

fn run_shard_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let addr = std::env::var("GREENMQTT_HTTP_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse::<SocketAddr>()?;
    let (method, path, body, output_mode) = shard_command_request(args)?;
    let response = http_request_body(addr, &method, &path, body.as_deref())?;
    match output_mode {
        OutputMode::Json => println!("{response}"),
        OutputMode::Text => println!("{}", render_shard_response_text(&response)?),
    }
    Ok(())
}

async fn run_range_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let endpoint = range_control_endpoint()?;
    let response = execute_range_command_with_endpoint(args, &endpoint).await?;
    println!("{response}");
    Ok(())
}

async fn execute_range_command_with_endpoint(
    args: impl Iterator<Item = String>,
    endpoint: &str,
) -> anyhow::Result<String> {
    let (command, output_mode) = range_command_request(args)?;
    let client = RangeControlGrpcClient::connect(endpoint.to_string()).await?;
    let reply = match command {
        RangeCliCommand::Bootstrap { descriptor } => RangeCliReply {
            operation: "bootstrap".into(),
            ok: true,
            status: "bootstrapped".into(),
            reason: "Range bootstrapped and marked serving.".into(),
            range_id: Some(client.bootstrap_range(descriptor).await?),
            left_range_id: None,
            right_range_id: None,
        },
        RangeCliCommand::Split { range_id, split_key } => {
            let (left_range_id, right_range_id) = client.split_range(&range_id, split_key).await?;
            RangeCliReply {
                operation: "split".into(),
                ok: true,
                status: "split".into(),
                reason: "Range split completed; child ranges are now available.".into(),
                range_id: Some(range_id),
                left_range_id: Some(left_range_id),
                right_range_id: Some(right_range_id),
            }
        }
        RangeCliCommand::Merge {
            left_range_id,
            right_range_id,
        } => RangeCliReply {
            operation: "merge".into(),
            ok: true,
            status: "merged".into(),
            reason: "Sibling ranges were merged into a single serving range.".into(),
            range_id: Some(client.merge_ranges(&left_range_id, &right_range_id).await?),
            left_range_id: Some(left_range_id),
            right_range_id: Some(right_range_id),
        },
        RangeCliCommand::Drain { range_id } => {
            client.drain_range(&range_id).await?;
            RangeCliReply {
                operation: "drain".into(),
                ok: true,
                status: "draining".into(),
                reason: "Range marked draining; writes should migrate away before retirement.".into(),
                range_id: Some(range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::Retire { range_id } => {
            client.retire_range(&range_id).await?;
            RangeCliReply {
                operation: "retire".into(),
                ok: true,
                status: "retired".into(),
                reason: "Range retired; repeated retire requests are treated as already applied.".into(),
                range_id: Some(range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::Recover {
            range_id,
            new_leader_node_id,
        } => {
            client.recover_range(&range_id, new_leader_node_id).await?;
            RangeCliReply {
                operation: "recover".into(),
                ok: true,
                status: "recovered".into(),
                reason: "Recovery command accepted; the target leader is now authoritative or already was.".into(),
                range_id: Some(range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::ChangeReplicas {
            range_id,
            voters,
            learners,
        } => {
            client.change_replicas(&range_id, voters, learners).await?;
            RangeCliReply {
                operation: "change-replicas".into(),
                ok: true,
                status: "reconfiguring".into(),
                reason: "Replica change accepted; consult range health for staging or catch-up progress.".into(),
                range_id: Some(range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::TransferLeadership {
            range_id,
            target_node_id,
        } => {
            client.transfer_leadership(&range_id, target_node_id).await?;
            RangeCliReply {
                operation: "transfer-leadership".into(),
                ok: true,
                status: "leadership-updated".into(),
                reason: "Leadership transfer accepted; if the target already led the range this was a no-op.".into(),
                range_id: Some(range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
    };
    match output_mode {
        OutputMode::Json => Ok(serde_json::to_string(&reply)?),
        OutputMode::Text => Ok(render_range_response_text(&reply)),
    }
}

fn range_control_endpoint() -> anyhow::Result<String> {
    let raw = std::env::var("GREENMQTT_RANGE_CONTROL_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("GREENMQTT_RPC_BIND")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "127.0.0.1:50051".to_string());
    Ok(if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("http://{raw}")
    })
}

fn parse_service_shard_kind(value: &str) -> anyhow::Result<ServiceShardKind> {
    match value {
        "sessiondict" => Ok(ServiceShardKind::SessionDict),
        "dist" => Ok(ServiceShardKind::Dist),
        "inbox" => Ok(ServiceShardKind::Inbox),
        "inflight" => Ok(ServiceShardKind::Inflight),
        "retain" => Ok(ServiceShardKind::Retain),
        _ => anyhow::bail!("invalid range kind: {value}"),
    }
}

fn parse_node_id_list(value: &str) -> anyhow::Result<Vec<u64>> {
    value
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            item.trim()
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("invalid node id: {}", item.trim()))
        })
        .collect()
}

fn render_range_response_text(reply: &RangeCliReply) -> String {
    match (
        reply.range_id.as_deref(),
        reply.left_range_id.as_deref(),
        reply.right_range_id.as_deref(),
    ) {
        (Some(range_id), Some(left), Some(right)) => {
            format!(
                "ok operation={} status={} reason=\"{}\" range_id={} left_range_id={} right_range_id={}",
                reply.operation, reply.status, reply.reason, range_id, left, right
            )
        }
        (Some(range_id), _, _) => format!(
            "ok operation={} status={} reason=\"{}\" range_id={}",
            reply.operation, reply.status, reply.reason, range_id
        ),
        _ => format!(
            "ok operation={} status={} reason=\"{}\"",
            reply.operation, reply.status, reply.reason
        ),
    }
}

fn range_command_request(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<(RangeCliCommand, OutputMode)> {
    let subcommand = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing range subcommand"))?;
    match subcommand.as_str() {
        "bootstrap" => {
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut start_key = None;
            let mut end_key = None;
            let mut epoch = 1u64;
            let mut config_version = 1u64;
            let mut leader_node_id = None;
            let mut voters = None;
            let mut learners = Vec::new();
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--range-id" => range_id = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --range-id"))?),
                    "--kind" => kind = Some(parse_service_shard_kind(&args.next().ok_or_else(|| anyhow::anyhow!("missing value for --kind"))?)?),
                    "--tenant-id" => tenant_id = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?),
                    "--scope" => scope = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?),
                    "--start-key" => start_key = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --start-key"))?.into_bytes()),
                    "--end-key" => end_key = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --end-key"))?.into_bytes()),
                    "--epoch" => epoch = args.next().ok_or_else(|| anyhow::anyhow!("missing value for --epoch"))?.parse()?,
                    "--config-version" => config_version = args.next().ok_or_else(|| anyhow::anyhow!("missing value for --config-version"))?.parse()?,
                    "--leader-node-id" => leader_node_id = Some(args.next().ok_or_else(|| anyhow::anyhow!("missing value for --leader-node-id"))?.parse()?),
                    "--voters" => voters = Some(parse_node_id_list(&args.next().ok_or_else(|| anyhow::anyhow!("missing value for --voters"))?)?),
                    "--learners" => learners = parse_node_id_list(&args.next().ok_or_else(|| anyhow::anyhow!("missing value for --learners"))?)?,
                    "--output" => output = parse_output_mode(&args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?)?,
                    _ => anyhow::bail!("unknown range bootstrap flag: {flag}"),
                }
            }
            let voters = voters.ok_or_else(|| anyhow::anyhow!("bootstrap requires --voters"))?;
            let leader = leader_node_id.or_else(|| voters.first().copied());
            Ok((
                RangeCliCommand::Bootstrap {
                    descriptor: ReplicatedRangeDescriptor::new(
                        range_id.ok_or_else(|| anyhow::anyhow!("bootstrap requires --range-id"))?,
                        ServiceShardKey {
                            kind: kind.ok_or_else(|| anyhow::anyhow!("bootstrap requires --kind"))?,
                            tenant_id: tenant_id.ok_or_else(|| anyhow::anyhow!("bootstrap requires --tenant-id"))?,
                            scope: scope.ok_or_else(|| anyhow::anyhow!("bootstrap requires --scope"))?,
                        },
                        RangeBoundary::new(start_key, end_key),
                        epoch,
                        config_version,
                        leader,
                        voters
                            .iter()
                            .copied()
                            .map(|node_id| {
                                RangeReplica::new(node_id, ReplicaRole::Voter, ReplicaSyncState::Replicating)
                            })
                            .chain(learners.iter().copied().map(|node_id| {
                                RangeReplica::new(node_id, ReplicaRole::Learner, ReplicaSyncState::Replicating)
                            }))
                            .collect(),
                        0,
                        0,
                        ServiceShardLifecycle::Bootstrapping,
                    ),
                },
                output,
            ))
        }
        "split" => {
            let range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let split_key = args.next().ok_or_else(|| anyhow::anyhow!("missing split_key"))?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range split flag: {flag}"),
                }
            }
            Ok((RangeCliCommand::Split { range_id, split_key: split_key.into_bytes() }, output))
        }
        "merge" => {
            let left_range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing left_range_id"))?;
            let right_range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing right_range_id"))?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range merge flag: {flag}"),
                }
            }
            Ok((RangeCliCommand::Merge { left_range_id, right_range_id }, output))
        }
        "drain" | "retire" => {
            let range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range {subcommand} flag: {flag}"),
                }
            }
            let command = if subcommand == "drain" {
                RangeCliCommand::Drain { range_id }
            } else {
                RangeCliCommand::Retire { range_id }
            };
            Ok((command, output))
        }
        "recover" => {
            let range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let new_leader_node_id = args.next().ok_or_else(|| anyhow::anyhow!("missing new_leader_node_id"))?.parse()?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range recover flag: {flag}"),
                }
            }
            Ok((RangeCliCommand::Recover { range_id, new_leader_node_id }, output))
        }
        "change-replicas" => {
            let range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let mut voters = None;
            let mut learners = Vec::new();
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--voters" => {
                        voters = Some(parse_node_id_list(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --voters"))?,
                        )?)
                    }
                    "--learners" => {
                        learners = parse_node_id_list(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --learners"))?,
                        )?
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range change-replicas flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::ChangeReplicas {
                    range_id,
                    voters: voters.ok_or_else(|| anyhow::anyhow!("change-replicas requires --voters"))?,
                    learners,
                },
                output,
            ))
        }
        "transfer-leadership" => {
            let range_id = args.next().ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let target_node_id = args.next().ok_or_else(|| anyhow::anyhow!("missing target_node_id"))?.parse()?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args.next().ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range transfer-leadership flag: {flag}"),
                }
            }
            Ok((RangeCliCommand::TransferLeadership { range_id, target_node_id }, output))
        }
        _ => anyhow::bail!("unknown range subcommand: {subcommand}"),
    }
}

fn shard_command_request(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<(String, String, Option<String>, OutputMode)> {
    let subcommand = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing shard subcommand"))?;
    match subcommand.as_str() {
        "ls" => {
            let mut query = Vec::new();
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--kind" => {
                        let value = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --kind"))?;
                        query.push(format!("kind={value}"));
                    }
                    "--tenant-id" => {
                        let value = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?;
                        query.push(format!("tenant_id={value}"));
                    }
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard ls flag: {flag}"),
                }
            }
            let path = if query.is_empty() {
                "/v1/shards".to_string()
            } else {
                format!("/v1/shards?{}", query.join("&"))
            };
            Ok(("GET".into(), path, None, output_mode))
        }
        "audit" => {
            let mut limit = None;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--limit" => {
                        limit = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --limit"))?,
                        );
                    }
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard audit flag: {flag}"),
                }
            }
            let path = match limit {
                Some(limit) => format!("/v1/audit?shard_only=true&limit={limit}"),
                None => "/v1/audit?shard_only=true".to_string(),
            };
            Ok(("GET".into(), path, None, output_mode))
        }
        "drain" => {
            let kind = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard kind"))?;
            let tenant_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing tenant_id"))?;
            let scope = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard scope"))?;
            let mut dry_run = false;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--dry-run" => dry_run = true,
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard drain flag: {flag}"),
                }
            }
            Ok((
                "POST".into(),
                format!("/v1/shards/{kind}/{tenant_id}/{scope}/drain"),
                Some(format!(r#"{{"dry_run":{dry_run}}}"#)),
                output_mode,
            ))
        }
        "move" | "failover" | "catch-up" | "repair" => {
            let kind = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard kind"))?;
            let tenant_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing tenant_id"))?;
            let scope = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing shard scope"))?;
            let target_node_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing target_node_id"))?;
            let mut dry_run = false;
            let mut output_mode = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--dry-run" => dry_run = true,
                    "--output" => {
                        output_mode = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?;
                    }
                    _ => anyhow::bail!("unknown shard {subcommand} flag: {flag}"),
                }
            }
            let action = match subcommand.as_str() {
                "move" => "move",
                "failover" => "failover",
                "catch-up" => "catch-up",
                _ => "repair",
            };
            Ok((
                "POST".into(),
                format!("/v1/shards/{kind}/{tenant_id}/{scope}/{action}"),
                Some(format!(
                    r#"{{"target_node_id":{target_node_id},"dry_run":{dry_run}}}"#
                )),
                output_mode,
            ))
        }
        _ => anyhow::bail!("unknown shard subcommand: {subcommand}"),
    }
}

fn parse_output_mode(value: &str) -> anyhow::Result<OutputMode> {
    match value {
        "json" => Ok(OutputMode::Json),
        "text" => Ok(OutputMode::Text),
        _ => anyhow::bail!("invalid output mode: {value}"),
    }
}

fn render_shard_response_text(body: &str) -> anyhow::Result<String> {
    if let Ok(assignments) =
        serde_json::from_str::<Vec<greenmqtt_core::ServiceShardAssignment>>(body)
    {
        return Ok(assignments
            .into_iter()
            .map(|assignment| {
                format!(
                    "{} tenant={} scope={} owner={} epoch={} fence={} lifecycle={:?}",
                    format!("{:?}", assignment.shard.kind).to_lowercase(),
                    assignment.shard.tenant_id,
                    assignment.shard.scope,
                    assignment.owner_node_id(),
                    assignment.epoch,
                    assignment.fencing_token,
                    assignment.lifecycle,
                )
            })
            .collect::<Vec<_>>()
            .join("\n"));
    }
    if let Ok(assignment) =
        serde_json::from_str::<Option<greenmqtt_core::ServiceShardAssignment>>(body)
    {
        return Ok(assignment
            .map(|assignment| {
                format!(
                    "{} tenant={} scope={} owner={} epoch={} fence={} lifecycle={:?}",
                    format!("{:?}", assignment.shard.kind).to_lowercase(),
                    assignment.shard.tenant_id,
                    assignment.shard.scope,
                    assignment.owner_node_id(),
                    assignment.epoch,
                    assignment.fencing_token,
                    assignment.lifecycle,
                )
            })
            .unwrap_or_else(|| "shard not found".to_string()));
    }
    let reply: ShardActionEnvelope = serde_json::from_str(body)?;
    let previous = reply
        .previous
        .as_ref()
        .map(|assignment| {
            format!(
                "{}:{} owner={} epoch={} fence={} lifecycle={:?}",
                assignment.shard.tenant_id,
                assignment.shard.scope,
                assignment.owner_node_id(),
                assignment.epoch,
                assignment.fencing_token,
                assignment.lifecycle,
            )
        })
        .unwrap_or_else(|| "none".to_string());
    let current = reply
        .current
        .as_ref()
        .map(|assignment| {
            format!(
                "{}:{} owner={} epoch={} fence={} lifecycle={:?}",
                assignment.shard.tenant_id,
                assignment.shard.scope,
                assignment.owner_node_id(),
                assignment.epoch,
                assignment.fencing_token,
                assignment.lifecycle,
            )
        })
        .unwrap_or_else(|| "none".to_string());
    Ok(format!("previous={previous}\ncurrent={current}"))
}

fn http_request_body(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    let payload = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(request.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("invalid http response"))?;
    Ok(body.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "demo".to_string());
    if mode == "shard" {
        return run_shard_command(args);
    }
    if mode == "range" {
        return run_range_command(args).await;
    }

    let node_id = std::env::var("GREENMQTT_NODE_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let state_endpoint = std::env::var("GREENMQTT_STATE_ENDPOINT").ok();
    let peer_forwarder = StaticPeerForwarder::default();
    configure_peers(&peer_forwarder).await?;
    let data_dir = std::env::var("GREENMQTT_DATA_DIR").ok();
    let storage_backend = std::env::var("GREENMQTT_STORAGE_BACKEND")
        .unwrap_or_else(|_| "sled".to_string())
        .to_lowercase();
    let redis_url = std::env::var("GREENMQTT_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let output_mode = output_mode();
    let mut local_range_host: Option<Arc<dyn KvRangeHost>> = None;
    let mut local_range_runtime: Option<Arc<ReplicaRuntime>> = None;
    let (default_sessiondict, default_dist, default_inbox, default_retain): AppStateServices =
        match state_endpoint.as_deref() {
            Some(endpoint) => (
                Arc::new(SessionDictGrpcClient::connect(endpoint).await?),
                Arc::new(DistGrpcClient::connect(endpoint).await?),
                Arc::new(InboxGrpcClient::connect(endpoint).await?),
                Arc::new(RetainGrpcClient::connect(endpoint).await?),
            ),
            None => {
                if storage_backend == "redis" {
                    redis_services(&redis_url).await?
                } else {
                    match data_dir {
                        Some(ref dir) if storage_backend == "rocksdb" => {
                            let stack = range_scoped_rocksdb_stack(PathBuf::from(dir), node_id).await?;
                            local_range_host = Some(stack.range_host.clone());
                            local_range_runtime = Some(stack.range_runtime.clone());
                            (stack.sessiondict, stack.dist, stack.inbox, stack.retain)
                        }
                        Some(ref dir) => {
                            durable_services(PathBuf::from(dir), &storage_backend, node_id).await?
                        }
                        None => (
                            Arc::new(SessionDictHandle::default()),
                            Arc::new(DistHandle::default()),
                            Arc::new(InboxHandle::default()),
                            Arc::new(RetainHandle::default()),
                        ),
                    }
                }
            }
        };

    if mode == "state-serve" {
        anyhow::ensure!(
            state_endpoint.is_none(),
            "state-serve does not support GREENMQTT_STATE_ENDPOINT"
        );
        let rpc_bind: SocketAddr = std::env::var("GREENMQTT_RPC_BIND")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()?;
        println!("greenmqtt state grpc listening on {rpc_bind}");
        RpcRuntime {
            sessiondict: default_sessiondict,
            dist: default_dist,
            inbox: default_inbox,
            retain: default_retain,
            peer_sink: Arc::new(greenmqtt_rpc::NoopDeliverySink),
            assignment_registry: None,
            range_host: local_range_host,
            range_runtime: local_range_runtime,
        }
        .serve(rpc_bind)
        .await?;
        return Ok(());
    }

    let sessiondict =
        configured_sessiondict_service(default_sessiondict, state_endpoint.as_deref()).await?;
    let dist = configured_dist_service(default_dist, state_endpoint.as_deref()).await?;
    let inbox = configured_inbox_service(default_inbox, state_endpoint.as_deref()).await?;
    let retain = configured_retain_service(default_retain, state_endpoint.as_deref()).await?;

    let tcp_listeners = if mode == "serve" {
        listener_specs_from_env(
            "GREENMQTT_MQTT_BIND",
            "GREENMQTT_MQTT_BINDS",
            "127.0.0.1:1883",
        )?
    } else {
        Vec::new()
    };
    let tls_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_TLS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let ws_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_WS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let wss_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_WSS_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let quic_listeners = if mode == "serve" {
        std::env::var("GREENMQTT_QUIC_BINDS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|raw| parse_listener_specs(&raw))
            .transpose()?
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let (auth, acl, hooks) = if mode == "serve" {
        let profiles = collect_listener_profiles(&[
            &tcp_listeners,
            &tls_listeners,
            &ws_listeners,
            &wss_listeners,
            &quic_listeners,
        ]);
        configured_listener_profiles(node_id, &profiles)?
    } else {
        (
            PortMappedAuth::new(
                "default",
                HashMap::from([(String::from("default"), configured_auth()?)]),
            ),
            PortMappedAcl::new(
                "default",
                HashMap::from([(String::from("default"), configured_acl()?)]),
            ),
            PortMappedEventHook::new(
                "default",
                HashMap::from([(String::from("default"), configured_hooks(node_id)?)]),
            ),
        )
    };

    let broker = Arc::new(BrokerRuntime::with_cluster(
        BrokerConfig {
            node_id,
            enable_tcp: true,
            enable_tls: true,
            enable_ws: true,
            enable_wss: true,
            enable_quic: true,
            server_keep_alive_secs: std::env::var("GREENMQTT_SERVER_KEEP_ALIVE_SECS")
                .ok()
                .map(|value| value.parse())
                .transpose()?,
            max_packet_size: std::env::var("GREENMQTT_MAX_PACKET_SIZE")
                .ok()
                .map(|value| value.parse())
                .transpose()?,
            response_information: std::env::var("GREENMQTT_RESPONSE_INFORMATION")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| Some("greenmqtt".to_string())),
            server_reference: std::env::var("GREENMQTT_SERVER_REFERENCE")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            audit_log_path: data_dir
                .as_ref()
                .map(|dir| PathBuf::from(dir).join("admin-audit.jsonl")),
        },
        auth,
        acl,
        hooks,
        Arc::new(peer_forwarder.clone()),
        sessiondict,
        dist,
        inbox,
        retain,
    ));
    let _ = broker.restore_peer_registry(&peer_forwarder).await?;

    if mode == "serve" {
        let http_bind: SocketAddr = std::env::var("GREENMQTT_HTTP_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse()?;
        let tls_bind = std::env::var("GREENMQTT_TLS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let tls_cert = std::env::var("GREENMQTT_TLS_CERT").ok();
        let tls_key = std::env::var("GREENMQTT_TLS_KEY").ok();
        let ws_bind = std::env::var("GREENMQTT_WS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let wss_bind = std::env::var("GREENMQTT_WSS_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let quic_bind = std::env::var("GREENMQTT_QUIC_BIND").ok().and_then(|bind| {
            parse_listener_specs(&bind)
                .ok()
                .and_then(|mut specs| specs.drain(..).next())
        });
        let rpc_bind: SocketAddr = std::env::var("GREENMQTT_RPC_BIND")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
            .parse()?;
        println!("greenmqtt http api listening on {http_bind}");
        for listener in &tcp_listeners {
            println!(
                "greenmqtt mqtt tcp listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        if let Some(listener) = &tls_bind {
            println!(
                "greenmqtt mqtt tls listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &tls_listeners {
            if tls_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt tls listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &ws_bind {
            println!(
                "greenmqtt mqtt ws listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &ws_listeners {
            if ws_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt ws listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &wss_bind {
            println!(
                "greenmqtt mqtt wss listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &wss_listeners {
            if wss_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt wss listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        if let Some(listener) = &quic_bind {
            println!(
                "greenmqtt mqtt quic listening on {} (profile={})",
                listener.bind, listener.profile
            );
        }
        for listener in &quic_listeners {
            if quic_bind.as_ref().map(|item| item.bind) != Some(listener.bind) {
                println!(
                    "greenmqtt mqtt quic listening on {} (profile={})",
                    listener.bind, listener.profile
                );
            }
        }
        println!("greenmqtt internal grpc listening on {rpc_bind}");
        let metadata_backend = std::env::var("GREENMQTT_METADATA_BACKEND")
            .unwrap_or_else(|_| "memory".to_string())
            .to_lowercase();
        let metadata_dir = std::env::var("GREENMQTT_METADATA_DATA_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                data_dir
                    .as_ref()
                    .map(|dir| PathBuf::from(dir).join("metadata"))
            });
        let (metadata_registry, shard_registry): (
            Arc<dyn MetadataRegistry>,
            Arc<dyn ShardControlRegistry>,
        ) = match metadata_backend.as_str() {
            "memory" => {
                let registry = Arc::new(StaticServiceEndpointRegistry::default());
                (registry.clone(), registry)
            }
            "rocksdb" => {
                let path = metadata_dir.ok_or_else(|| {
                    anyhow::anyhow!(
                        "GREENMQTT_METADATA_BACKEND=rocksdb requires GREENMQTT_METADATA_DATA_DIR or GREENMQTT_DATA_DIR"
                    )
                })?;
                let registry = Arc::new(PersistentMetadataRegistry::open(path).await?);
                (registry.clone(), registry)
            }
            "replicated" => {
                let range_endpoint = std::env::var("GREENMQTT_RANGE_ENDPOINT")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .or_else(|| state_endpoint.clone())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "GREENMQTT_METADATA_BACKEND=replicated requires GREENMQTT_RANGE_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
                        )
                    })?;
                let range_base = std::env::var("GREENMQTT_METADATA_RANGE_ID")
                    .unwrap_or_else(|_| "__metadata".to_string());
                let layout = MetadataPlaneLayout {
                    assignments_range_id: std::env::var(
                        "GREENMQTT_METADATA_ASSIGNMENTS_RANGE_ID",
                    )
                    .unwrap_or_else(|_| format!("{range_base}.assignments")),
                    members_range_id: std::env::var("GREENMQTT_METADATA_MEMBERS_RANGE_ID")
                        .unwrap_or_else(|_| format!("{range_base}.members")),
                    ranges_range_id: std::env::var("GREENMQTT_METADATA_RANGES_RANGE_ID")
                        .unwrap_or_else(|_| format!("{range_base}.ranges")),
                    balancers_range_id: std::env::var("GREENMQTT_METADATA_BALANCERS_RANGE_ID")
                        .unwrap_or_else(|_| format!("{range_base}.balancers")),
                };
                let registry = Arc::new(ReplicatedMetadataRegistry::with_layout(
                    Arc::new(KvRangeGrpcClient::connect(range_endpoint).await?),
                    layout,
                ));
                (registry.clone(), registry)
            }
            other => anyhow::bail!("unsupported GREENMQTT_METADATA_BACKEND: {other}"),
        };
        shard_registry
            .upsert_member(ClusterNodeMembership::new(
                node_id,
                1,
                ClusterNodeLifecycle::Serving,
                vec![
                    ServiceEndpoint::new(
                        ServiceKind::SessionDict,
                        node_id,
                        format!("http://{rpc_bind}"),
                    ),
                    ServiceEndpoint::new(ServiceKind::Dist, node_id, format!("http://{rpc_bind}")),
                    ServiceEndpoint::new(ServiceKind::Inbox, node_id, format!("http://{rpc_bind}")),
                    ServiceEndpoint::new(
                        ServiceKind::Retain,
                        node_id,
                        format!("http://{rpc_bind}"),
                    ),
                ],
            ))
            .await?;
        let metrics_handle = PrometheusBuilder::new().install_recorder()?;
        let http = if let Some(range_runtime) = local_range_runtime.clone() {
            HttpApi::with_peers_shards_metrics_and_ranges(
                broker.clone(),
                Arc::new(peer_forwarder.clone()),
                shard_registry.clone(),
                range_runtime,
                metrics_handle,
                http_bind,
            )
        } else {
            HttpApi::with_peers_shards_and_metrics(
                broker.clone(),
                Arc::new(peer_forwarder.clone()),
                shard_registry.clone(),
                metrics_handle,
                http_bind,
            )
        };
        let rpc = RpcRuntime {
            sessiondict: broker.sessiondict.clone(),
            dist: broker.dist.clone(),
            inbox: broker.inbox.clone(),
            retain: broker.retain.clone(),
            peer_sink: broker.clone(),
            assignment_registry: Some(metadata_registry),
            range_host: local_range_host.clone(),
            range_runtime: local_range_runtime.clone(),
        };
        let mut tasks = JoinSet::new();
        tasks.spawn(async move { http.start().await });
        tasks.spawn(async move { rpc.serve(rpc_bind).await });
        if let (Some(dist_runtime), Some(dist_maintenance)) = (
            replicated_dist_runtime_handle(state_endpoint.as_deref()).await?,
            configured_dist_maintenance()?,
        ) {
            spawn_dist_maintenance_task(&mut tasks, dist_runtime, dist_maintenance);
        }
        if let (Some(inbox_runtime), Some(inbox_maintenance)) = (
            replicated_inbox_runtime_handle(state_endpoint.as_deref()).await?,
            configured_inbox_maintenance()?,
        ) {
            spawn_inbox_maintenance_task(&mut tasks, inbox_runtime, inbox_maintenance);
        }
        if let (Some(retain_runtime), Some(retain_maintenance)) = (
            replicated_retain_runtime_handle(state_endpoint.as_deref()).await?,
            configured_retain_maintenance()?,
        ) {
            spawn_retain_maintenance_task(&mut tasks, retain_runtime, retain_maintenance);
        }
        for listener in tcp_listeners {
            let broker = broker.clone();
            tasks.spawn(async move {
                serve_tcp_with_profile(broker, listener.bind, listener.profile).await
            });
        }
        if let (Some(cert), Some(key)) = (tls_cert.clone(), tls_key.clone()) {
            let listeners = if tls_listeners.is_empty() {
                tls_bind.into_iter().collect::<Vec<_>>()
            } else {
                tls_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_tls_with_profile(broker, listener.bind, cert, key, listener.profile).await
                });
            }
        }
        {
            let listeners = if ws_listeners.is_empty() {
                ws_bind.into_iter().collect::<Vec<_>>()
            } else {
                ws_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                tasks.spawn(async move {
                    serve_ws_with_profile(broker, listener.bind, listener.profile).await
                });
            }
        }
        if let (Some(cert), Some(key)) = (tls_cert.clone(), tls_key.clone()) {
            let listeners = if wss_listeners.is_empty() {
                wss_bind.into_iter().collect::<Vec<_>>()
            } else {
                wss_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_wss_with_profile(broker, listener.bind, cert, key, listener.profile).await
                });
            }
        }
        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let listeners = if quic_listeners.is_empty() {
                quic_bind.into_iter().collect::<Vec<_>>()
            } else {
                quic_listeners
            };
            for listener in listeners {
                let broker = broker.clone();
                let cert = cert.clone();
                let key = key.clone();
                tasks.spawn(async move {
                    serve_quic_with_profile(broker, listener.bind, cert, key, listener.profile)
                        .await
                });
            }
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        return Ok(());
    }

    if mode == "bench" {
        let storage_backend_label = storage_backend_label(
            state_endpoint.as_deref(),
            data_dir.as_deref(),
            &storage_backend,
        );
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: std::env::var("GREENMQTT_BENCH_SCENARIO")
                .ok()
                .as_deref()
                .map(parse_bench_scenario)
                .transpose()?
                .unwrap_or(BenchScenario::Live),
        };
        let report = run_bench(broker, node_id, config, &storage_backend_label).await?;
        print_bench_report(&report, output_mode)?;
        validate_bench_report(&report, bench_thresholds_from_env())?;
        return Ok(());
    }

    if mode == "compare-bench" {
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: std::env::var("GREENMQTT_BENCH_SCENARIO")
                .ok()
                .as_deref()
                .map(parse_bench_scenario)
                .transpose()?
                .unwrap_or(BenchScenario::Live),
        };
        let backends = std::env::var("GREENMQTT_COMPARE_BACKENDS")
            .ok()
            .as_deref()
            .map(parse_compare_backends)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    "memory".to_string(),
                    "sled".to_string(),
                    "rocksdb".to_string(),
                ]
            });
        let report = run_compare_bench(node_id, config, &backends, &redis_url).await?;
        print_compare_bench_report(&report, output_mode)?;
        return Ok(());
    }

    if mode == "profile-bench" {
        anyhow::ensure!(
            state_endpoint.is_none(),
            "profile-bench does not support GREENMQTT_STATE_ENDPOINT"
        );
        let config = BenchConfig {
            subscribers: std::env::var("GREENMQTT_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            publishers: std::env::var("GREENMQTT_BENCH_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_BENCH_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            qos: std::env::var("GREENMQTT_BENCH_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
            scenario: BenchScenario::Live,
        };
        let scenarios = std::env::var("GREENMQTT_PROFILE_SCENARIOS")
            .ok()
            .as_deref()
            .map(parse_bench_scenarios)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    BenchScenario::Live,
                    BenchScenario::OfflineReplay,
                    BenchScenario::RetainedReplay,
                    BenchScenario::SharedLive,
                ]
            });
        let backend = if data_dir.is_some() {
            storage_backend.clone()
        } else {
            "memory".to_string()
        };
        let report = run_profile_bench(node_id, config, &scenarios, &backend, &redis_url).await?;
        print_profile_bench_report(&report, output_mode)?;
        return Ok(());
    }

    if mode == "soak" {
        let storage_backend_label = storage_backend_label(
            state_endpoint.as_deref(),
            data_dir.as_deref(),
            &storage_backend,
        );
        let config = SoakConfig {
            iterations: std::env::var("GREENMQTT_SOAK_ITERATIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            subscribers: std::env::var("GREENMQTT_SOAK_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            publishers: std::env::var("GREENMQTT_SOAK_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
            qos: std::env::var("GREENMQTT_SOAK_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
        };
        let report = run_soak(broker, node_id, config, &storage_backend_label).await?;
        print_soak_report(&report, output_mode)?;
        validate_soak_report(&report, soak_thresholds_from_env())?;
        return Ok(());
    }

    if mode == "compare-soak" {
        let config = SoakConfig {
            iterations: std::env::var("GREENMQTT_SOAK_ITERATIONS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
            subscribers: std::env::var("GREENMQTT_SOAK_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            publishers: std::env::var("GREENMQTT_SOAK_PUBLISHERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10),
            messages_per_publisher: std::env::var("GREENMQTT_SOAK_MESSAGES_PER_PUBLISHER")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(20),
            qos: std::env::var("GREENMQTT_SOAK_QOS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1),
        };
        let backends = std::env::var("GREENMQTT_COMPARE_BACKENDS")
            .ok()
            .as_deref()
            .map(parse_compare_backends)
            .transpose()?
            .unwrap_or_else(|| {
                vec![
                    "memory".to_string(),
                    "sled".to_string(),
                    "rocksdb".to_string(),
                ]
            });
        let report = run_compare_soak(node_id, config, &backends, &redis_url).await?;
        print_compare_soak_report(&report, output_mode)?;
        return Ok(());
    }

    let subscriber = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "subscriber".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await?;
    broker
        .subscribe(
            &subscriber.session.session_id,
            "devices/+/state",
            1,
            None,
            false,
            false,
            0,
            None,
        )
        .await?;
    broker.disconnect(&subscriber.session.session_id).await?;

    let publisher = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "bob".into(),
                client_id: "publisher".into(),
            },
            node_id: 1,
            kind: SessionKind::Transient,
            clean_start: true,
            session_expiry_interval_secs: None,
        })
        .await?;
    let outcome = broker
        .publish(
            &publisher.session.session_id,
            PublishRequest {
                topic: "devices/d1/state".into(),
                payload: b"up".to_vec().into(),
                qos: 1,
                retain: true,
                properties: PublishProperties::default(),
            },
        )
        .await?;

    let resumed = broker
        .connect(ConnectRequest {
            identity: ClientIdentity {
                tenant_id: "demo".into(),
                user_id: "alice".into(),
                client_id: "subscriber".into(),
            },
            node_id: 1,
            kind: SessionKind::Persistent,
            clean_start: false,
            session_expiry_interval_secs: None,
        })
        .await?;

    println!(
        "greenmqtt demo: matched_routes={}, offline_messages={}",
        outcome.matched_routes,
        resumed.offline_messages.len()
    );
    Ok(())
}

async fn run_bench<A, C, H>(
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
                broker
                    .disconnect(subscriber_sessions.last().unwrap())
                    .await?;
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

async fn run_compare_bench(
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

async fn run_compare_soak(
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

async fn run_profile_bench(
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

async fn run_soak<A, C, H>(
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

fn output_mode() -> OutputMode {
    match std::env::var("GREENMQTT_OUTPUT")
        .unwrap_or_else(|_| "text".to_string())
        .to_lowercase()
        .as_str()
    {
        "json" => OutputMode::Json,
        _ => OutputMode::Text,
    }
}

fn storage_backend_label(
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

fn bench_thresholds_from_env() -> BenchThresholds {
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

fn soak_thresholds_from_env() -> SoakThresholds {
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

fn parse_bench_scenario(value: &str) -> anyhow::Result<BenchScenario> {
    match value {
        "live" => Ok(BenchScenario::Live),
        "offline_replay" => Ok(BenchScenario::OfflineReplay),
        "retained_replay" => Ok(BenchScenario::RetainedReplay),
        "shared_live" => Ok(BenchScenario::SharedLive),
        other => anyhow::bail!("unsupported GREENMQTT_BENCH_SCENARIO: {other}"),
    }
}

fn parse_bench_scenarios(value: &str) -> anyhow::Result<Vec<BenchScenario>> {
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

fn parse_compare_backends(value: &str) -> anyhow::Result<Vec<String>> {
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

fn print_bench_report(report: &BenchReport, output_mode: OutputMode) -> anyhow::Result<()> {
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

fn print_compare_bench_report(
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

fn print_profile_bench_report(
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

fn print_compare_soak_report(
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

fn print_soak_report(report: &SoakReport, output_mode: OutputMode) -> anyhow::Result<()> {
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

fn validate_bench_report(report: &BenchReport, thresholds: BenchThresholds) -> anyhow::Result<()> {
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

fn validate_soak_report(report: &SoakReport, thresholds: SoakThresholds) -> anyhow::Result<()> {
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

fn configured_hooks_for_profile(
    node_id: u64,
    profile: &str,
) -> anyhow::Result<ConfiguredEventHook> {
    let mut hooks = Vec::new();
    let rewrite_rules =
        profile_env_var("GREENMQTT_TOPIC_REWRITE_RULES", profile).unwrap_or_default();
    if !rewrite_rules.trim().is_empty() {
        hooks.push(HookTarget::TopicRewrite(TopicRewriteEventHook::new(
            parse_topic_rewrite_rules(&rewrite_rules)?,
        )));
    }
    let rules = profile_env_var("GREENMQTT_BRIDGE_RULES", profile).unwrap_or_default();
    if !rules.trim().is_empty() {
        let timeout_ms = profile_env_var("GREENMQTT_BRIDGE_TIMEOUT_MS", profile)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(5_000);
        let fail_open = profile_env_var("GREENMQTT_BRIDGE_FAIL_OPEN", profile)
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no"
                )
            })
            .unwrap_or(true);
        let retries = profile_env_var("GREENMQTT_BRIDGE_RETRIES", profile)
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);
        let retry_delay_ms = profile_env_var("GREENMQTT_BRIDGE_RETRY_DELAY_MS", profile)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let max_inflight = profile_env_var("GREENMQTT_BRIDGE_MAX_INFLIGHT", profile)
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(16);
        hooks.push(HookTarget::Bridge(BridgeEventHook::with_options(
            format!("{BRIDGE_CLIENT_ID_PREFIX}{node_id}"),
            parse_bridge_rules(&rules)?,
            Duration::from_millis(timeout_ms),
            fail_open,
            retries,
            Duration::from_millis(retry_delay_ms),
            max_inflight,
        )));
    }
    if let Some(webhook) = configured_webhook(profile)? {
        hooks.push(HookTarget::WebHook(webhook));
    }
    Ok(ConfiguredEventHook::new(hooks))
}

fn configured_hooks(node_id: u64) -> anyhow::Result<ConfiguredEventHook> {
    configured_hooks_for_profile(node_id, "default")
}

fn parse_topic_rewrite_rules(raw: &str) -> anyhow::Result<Vec<TopicRewriteRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let entry = entry.trim();
            let (scope, rewrite_to) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid topic rewrite rule `{entry}`"))?;
            let (tenant_id, topic_filter) = scope
                .split_once('@')
                .map(|(tenant, filter)| (Some(tenant.trim().to_string()), filter.trim()))
                .unwrap_or((None, scope.trim()));
            anyhow::ensure!(
                !topic_filter.is_empty(),
                "topic rewrite filter must not be empty"
            );
            anyhow::ensure!(
                !rewrite_to.trim().is_empty(),
                "topic rewrite target must not be empty"
            );
            Ok(TopicRewriteRule {
                tenant_id,
                topic_filter: topic_filter.to_string(),
                rewrite_to: rewrite_to.trim().to_string(),
            })
        })
        .collect()
}

fn normalize_profile_name(profile: &str) -> String {
    profile
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn profile_env_var(key: &str, profile: &str) -> Option<String> {
    if profile == "default" {
        std::env::var(key).ok()
    } else {
        std::env::var(format!("{key}__{}", normalize_profile_name(profile)))
            .ok()
            .or_else(|| std::env::var(key).ok())
    }
}

fn parse_listener_specs(raw: &str) -> anyhow::Result<Vec<ListenerSpec>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let entry = entry.trim();
            let (bind, profile) = match entry.rsplit_once('@') {
                Some((bind, profile)) if !profile.trim().is_empty() => (bind, profile.trim()),
                _ => (entry, "default"),
            };
            Ok(ListenerSpec {
                bind: bind.parse()?,
                profile: profile.to_string(),
            })
        })
        .collect()
}

fn listener_specs_from_env(
    single_key: &str,
    multi_key: &str,
    default_bind: &str,
) -> anyhow::Result<Vec<ListenerSpec>> {
    if let Ok(raw) = std::env::var(multi_key) {
        if !raw.trim().is_empty() {
            return parse_listener_specs(&raw);
        }
    }
    let bind = std::env::var(single_key).unwrap_or_else(|_| default_bind.to_string());
    parse_listener_specs(&bind)
}

fn collect_listener_profiles(listener_groups: &[&[ListenerSpec]]) -> Vec<String> {
    let mut profiles = HashSet::new();
    profiles.insert("default".to_string());
    for group in listener_groups {
        for listener in *group {
            profiles.insert(listener.profile.clone());
        }
    }
    let mut profiles = profiles.into_iter().collect::<Vec<_>>();
    profiles.sort();
    profiles
}

fn configured_listener_profiles(
    node_id: u64,
    profiles: &[String],
) -> anyhow::Result<(PortMappedAuth, PortMappedAcl, PortMappedEventHook)> {
    let mut auth_profiles = HashMap::new();
    let mut acl_profiles = HashMap::new();
    let mut hook_profiles = HashMap::new();
    for profile in profiles {
        auth_profiles.insert(profile.clone(), configured_auth_for_profile(profile)?);
        acl_profiles.insert(profile.clone(), configured_acl_for_profile(profile)?);
        hook_profiles.insert(
            profile.clone(),
            configured_hooks_for_profile(node_id, profile)?,
        );
    }
    Ok((
        PortMappedAuth::new("default", auth_profiles),
        PortMappedAcl::new("default", acl_profiles),
        PortMappedEventHook::new("default", hook_profiles),
    ))
}

fn default_listener_profiles(
    auth: ConfiguredAuth,
    acl: ConfiguredAcl,
    hooks: ConfiguredEventHook,
) -> (PortMappedAuth, PortMappedAcl, PortMappedEventHook) {
    (
        PortMappedAuth::new("default", HashMap::from([(String::from("default"), auth)])),
        PortMappedAcl::new("default", HashMap::from([(String::from("default"), acl)])),
        PortMappedEventHook::new("default", HashMap::from([(String::from("default"), hooks)])),
    )
}

fn configured_webhook(profile: &str) -> anyhow::Result<Option<WebHookEventHook>> {
    let url = profile_env_var("GREENMQTT_WEBHOOK_URL", profile).unwrap_or_default();
    if url.trim().is_empty() {
        return Ok(None);
    }
    let events = profile_env_var("GREENMQTT_WEBHOOK_EVENTS", profile)
        .unwrap_or_else(|| "publish,offline,retain".to_string());
    let mut connect = false;
    let mut disconnect = false;
    let mut subscribe = false;
    let mut unsubscribe = false;
    let mut publish = false;
    let mut offline = false;
    let mut retain = false;
    for event in events
        .split(',')
        .map(str::trim)
        .filter(|event| !event.is_empty())
    {
        match event {
            "connect" => connect = true,
            "disconnect" => disconnect = true,
            "subscribe" => subscribe = true,
            "unsubscribe" => unsubscribe = true,
            "publish" => publish = true,
            "offline" => offline = true,
            "retain" => retain = true,
            other => anyhow::bail!("unsupported GREENMQTT_WEBHOOK_EVENTS value `{other}`"),
        }
    }
    anyhow::ensure!(
        connect || disconnect || subscribe || unsubscribe || publish || offline || retain,
        "GREENMQTT_WEBHOOK_EVENTS must enable at least one event"
    );
    Ok(Some(WebHookEventHook::new(WebHookConfig {
        url: url.trim().to_string(),
        connect,
        disconnect,
        subscribe,
        unsubscribe,
        publish,
        offline,
        retain,
    })))
}

fn configured_auth_for_profile(profile: &str) -> anyhow::Result<ConfiguredAuth> {
    let denied_identities = parse_identity_matchers(
        &profile_env_var("GREENMQTT_AUTH_DENY_IDENTITIES", profile).unwrap_or_default(),
    )?;
    let enhanced_auth_method =
        profile_env_var("GREENMQTT_ENHANCED_AUTH_METHOD", profile).unwrap_or_default();
    if !enhanced_auth_method.trim().is_empty() {
        let raw_identities =
            profile_env_var("GREENMQTT_AUTH_IDENTITIES", profile).unwrap_or_default();
        let identities = if raw_identities.trim().is_empty() {
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }]
        } else {
            parse_identity_matchers(&raw_identities)?
        };
        let challenge = profile_env_var("GREENMQTT_ENHANCED_AUTH_CHALLENGE", profile)
            .unwrap_or_else(|| "challenge".to_string());
        let response = profile_env_var("GREENMQTT_ENHANCED_AUTH_RESPONSE", profile)
            .unwrap_or_else(|| "response".to_string());
        return Ok(ConfiguredAuth::EnhancedStatic(
            StaticEnhancedAuthProvider::with_denied(
                identities,
                denied_identities,
                enhanced_auth_method.trim().to_string(),
                challenge.into_bytes(),
                response.into_bytes(),
            ),
        ));
    }
    let raw = profile_env_var("GREENMQTT_AUTH_IDENTITIES", profile).unwrap_or_default();
    if !raw.trim().is_empty() || !denied_identities.is_empty() {
        let allowed_identities = if raw.trim().is_empty() {
            vec![IdentityMatcher {
                tenant_id: "*".to_string(),
                user_id: "*".to_string(),
                client_id: "*".to_string(),
            }]
        } else {
            parse_identity_matchers(&raw)?
        };
        return Ok(ConfiguredAuth::Static(StaticAuthProvider::with_denied(
            allowed_identities,
            denied_identities,
        )));
    }
    let http_auth_url = profile_env_var("GREENMQTT_HTTP_AUTH_URL", profile).unwrap_or_default();
    if !http_auth_url.trim().is_empty() {
        return Ok(ConfiguredAuth::Http(HttpAuthProvider::new(
            HttpAuthConfig {
                url: http_auth_url.trim().to_string(),
            },
        )));
    }
    Ok(ConfiguredAuth::default())
}

fn configured_auth() -> anyhow::Result<ConfiguredAuth> {
    configured_auth_for_profile("default")
}

fn configured_acl_for_profile(profile: &str) -> anyhow::Result<ConfiguredAcl> {
    let raw = profile_env_var("GREENMQTT_ACL_RULES", profile).unwrap_or_default();
    let acl_default_allow = profile_env_var("GREENMQTT_ACL_DEFAULT_ALLOW", profile)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if !raw.trim().is_empty() {
        return Ok(ConfiguredAcl::Static(
            StaticAclProvider::with_default_decision(parse_acl_rules(&raw)?, acl_default_allow),
        ));
    }
    let http_acl_url = profile_env_var("GREENMQTT_HTTP_ACL_URL", profile).unwrap_or_default();
    if !http_acl_url.trim().is_empty() {
        return Ok(ConfiguredAcl::Http(HttpAclProvider::new(HttpAclConfig {
            url: http_acl_url.trim().to_string(),
        })));
    }
    Ok(ConfiguredAcl::default())
}

fn configured_acl() -> anyhow::Result<ConfiguredAcl> {
    configured_acl_for_profile("default")
}

fn parse_identity_matchers(raw: &str) -> anyhow::Result<Vec<IdentityMatcher>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(parse_identity_matcher)
        .collect()
}

fn parse_identity_matcher(entry: &str) -> anyhow::Result<IdentityMatcher> {
    let parts: Vec<_> = entry.trim().split(':').collect();
    anyhow::ensure!(
        parts.len() == 3,
        "identity matcher must be tenant:user:client, got `{entry}`"
    );
    for part in &parts {
        anyhow::ensure!(
            !part.trim().is_empty(),
            "identity matcher segments must not be empty"
        );
    }
    Ok(IdentityMatcher {
        tenant_id: parts[0].trim().to_string(),
        user_id: parts[1].trim().to_string(),
        client_id: parts[2].trim().to_string(),
    })
}

fn parse_acl_rules(raw: &str) -> anyhow::Result<Vec<AclRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (lhs, topic_filter) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid acl rule `{entry}`"))?;
            let (decision_action, identity) = lhs
                .split_once('@')
                .ok_or_else(|| anyhow::anyhow!("invalid acl rule `{entry}`"))?;
            let (decision, action) = parse_acl_decision_action(decision_action.trim())?;
            anyhow::ensure!(
                !topic_filter.trim().is_empty(),
                "acl topic filter must not be empty"
            );
            Ok(AclRule {
                decision,
                action,
                identity: parse_identity_matcher(identity.trim())?,
                topic_filter: topic_filter.trim().to_string(),
            })
        })
        .collect()
}

fn parse_acl_decision_action(raw: &str) -> anyhow::Result<(AclDecision, AclAction)> {
    match raw {
        "allow-sub" => Ok((AclDecision::Allow, AclAction::Subscribe)),
        "deny-sub" => Ok((AclDecision::Deny, AclAction::Subscribe)),
        "allow-pub" => Ok((AclDecision::Allow, AclAction::Publish)),
        "deny-pub" => Ok((AclDecision::Deny, AclAction::Publish)),
        _ => anyhow::bail!("unsupported acl rule action `{raw}`"),
    }
}

fn parse_bridge_rules(raw: &str) -> anyhow::Result<Vec<BridgeRule>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (lhs, remote_addr) = entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid bridge rule `{entry}`"))?;
            let (tenant_id, scoped_filter) = match lhs.split_once('@') {
                Some((tenant_id, topic_filter)) => (
                    Some(tenant_id.trim().to_string()),
                    topic_filter.trim().to_string(),
                ),
                None => (None, lhs.trim().to_string()),
            };
            let (topic_filter, rewrite_to) = match scoped_filter.split_once("->") {
                Some((topic_filter, rewrite_to)) => (
                    topic_filter.trim().to_string(),
                    Some(rewrite_to.trim().to_string()),
                ),
                None => (scoped_filter, None),
            };
            anyhow::ensure!(
                !topic_filter.is_empty(),
                "bridge topic filter must not be empty"
            );
            if let Some(rewrite_to) = &rewrite_to {
                anyhow::ensure!(
                    !rewrite_to.is_empty(),
                    "bridge rewrite target must not be empty"
                );
            }
            anyhow::ensure!(
                !remote_addr.trim().is_empty(),
                "bridge remote address must not be empty"
            );
            Ok(BridgeRule {
                tenant_id,
                topic_filter,
                rewrite_to,
                remote_addr: remote_addr.trim().to_string(),
            })
        })
        .collect()
}

async fn configure_peers(forwarder: &StaticPeerForwarder) -> anyhow::Result<()> {
    let peers = match std::env::var("GREENMQTT_PEERS") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(()),
    };

    for item in peers
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (node_id, endpoint) = item
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid GREENMQTT_PEERS entry: {item}"))?;
        forwarder
            .connect_node(node_id.parse()?, endpoint.trim().to_string())
            .await?;
    }
    Ok(())
}

#[derive(Clone)]
struct LocalKvRangeExecutor {
    engine: Arc<dyn KvEngine>,
}

#[async_trait]
impl KvRangeExecutor for LocalKvRangeExecutor {
    async fn get(&self, range_id: &str, key: &[u8]) -> anyhow::Result<Option<bytes::Bytes>> {
        let range = self.engine.open_range(range_id).await?;
        range.reader().get(key).await
    }

    async fn scan(
        &self,
        range_id: &str,
        boundary: Option<RangeBoundary>,
        limit: usize,
    ) -> anyhow::Result<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let range = self.engine.open_range(range_id).await?;
        range
            .reader()
            .scan(&boundary.unwrap_or_else(RangeBoundary::full), limit)
            .await
    }

    async fn apply(&self, range_id: &str, mutations: Vec<KvMutation>) -> anyhow::Result<()> {
        let range = self.engine.open_range(range_id).await?;
        range.writer().apply(mutations).await
    }

    async fn checkpoint(
        &self,
        range_id: &str,
        checkpoint_id: &str,
    ) -> anyhow::Result<KvRangeCheckpoint> {
        let range = self.engine.open_range(range_id).await?;
        range.checkpoint(checkpoint_id).await
    }

    async fn snapshot(&self, range_id: &str) -> anyhow::Result<KvRangeSnapshot> {
        let range = self.engine.open_range(range_id).await?;
        range.snapshot().await
    }
}

#[derive(Clone, Default)]
struct SingleNodeServiceRangeRouter {
    descriptors: Arc<RwLock<BTreeMap<String, ReplicatedRangeDescriptor>>>,
}

impl SingleNodeServiceRangeRouter {
    fn new(descriptors: impl IntoIterator<Item = ReplicatedRangeDescriptor>) -> Self {
        let router = Self::default();
        {
            let mut guard = router
                .descriptors
                .write()
                .expect("single-node router poisoned");
            for descriptor in descriptors {
                guard.insert(descriptor.id.clone(), descriptor);
            }
        }
        router
    }
}

#[async_trait]
impl KvRangeRouter for SingleNodeServiceRangeRouter {
    async fn upsert(&self, descriptor: ReplicatedRangeDescriptor) -> anyhow::Result<()> {
        self.descriptors
            .write()
            .expect("single-node router poisoned")
            .insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    async fn remove(&self, range_id: &str) -> anyhow::Result<Option<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .write()
            .expect("single-node router poisoned")
            .remove(range_id))
    }

    async fn list(&self) -> anyhow::Result<Vec<ReplicatedRangeDescriptor>> {
        Ok(self
            .descriptors
            .read()
            .expect("single-node router poisoned")
            .values()
            .cloned()
            .collect())
    }

    async fn by_id(
        &self,
        range_id: &str,
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("single-node router poisoned")
            .get(range_id)
            .cloned()
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_key(
        &self,
        shard: &ServiceShardKey,
        key: &[u8],
    ) -> anyhow::Result<Option<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("single-node router poisoned")
            .values()
            .find(|descriptor| descriptor.shard.kind == shard.kind && descriptor.contains_key(key))
            .cloned()
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor))
    }

    async fn route_shard(
        &self,
        shard: &ServiceShardKey,
    ) -> anyhow::Result<Vec<greenmqtt_kv_client::RangeRoute>> {
        Ok(self
            .descriptors
            .read()
            .expect("single-node router poisoned")
            .values()
            .filter(|descriptor| descriptor.shard.kind == shard.kind)
            .cloned()
            .map(greenmqtt_kv_client::RangeRoute::from_descriptor)
            .collect())
    }
}

fn range_engine_root(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("range-engine")
}

fn legacy_rocksdb_store_dirs(data_dir: &std::path::Path) -> [PathBuf; 6] {
    [
        data_dir.join("sessions"),
        data_dir.join("routes"),
        data_dir.join("subscriptions"),
        data_dir.join("inbox"),
        data_dir.join("inflight"),
        data_dir.join("retain"),
    ]
}

fn single_node_range_descriptors(node_id: u64) -> Vec<ReplicatedRangeDescriptor> {
    [
        (
            "__sessiondict",
            ServiceShardKey {
                kind: ServiceShardKind::SessionDict,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__dist",
            ServiceShardKey {
                kind: ServiceShardKind::Dist,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__retain",
            ServiceShardKey {
                kind: ServiceShardKind::Retain,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inbox",
            ServiceShardKey {
                kind: ServiceShardKind::Inbox,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
        (
            "__inflight",
            ServiceShardKey {
                kind: ServiceShardKind::Inflight,
                tenant_id: "*".into(),
                scope: "*".into(),
            },
        ),
    ]
    .into_iter()
    .map(|(range_id, shard)| {
        ReplicatedRangeDescriptor::new(
            range_id,
            shard,
            RangeBoundary::full(),
            1,
            1,
            Some(node_id),
            vec![RangeReplica::new(
                node_id,
                ReplicaRole::Voter,
                ReplicaSyncState::Replicating,
            )],
            0,
            0,
            ServiceShardLifecycle::Serving,
        )
    })
    .collect()
}

async fn range_scoped_rocksdb_stack(
    data_dir: PathBuf,
    node_id: u64,
) -> anyhow::Result<LocalRangeRuntimeStack> {
    let root = range_engine_root(&data_dir);
    let has_legacy_layout = legacy_rocksdb_store_dirs(&data_dir)
        .into_iter()
        .any(|path| path.exists());
    anyhow::ensure!(
        root.exists() || !has_legacy_layout,
        "legacy RocksDB store layout detected in {}; migrate to range-engine layout before using GREENMQTT_STORAGE_BACKEND=rocksdb",
        data_dir.display()
    );

    let engine = Arc::new(RocksDbKvEngine::open(&root)?);
    let descriptors = single_node_range_descriptors(node_id);
    let host = Arc::new(MemoryKvRangeHost::default());
    for descriptor in &descriptors {
        engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::from(engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::single_node(node_id, &descriptor.id));
        raft_impl.recover().await?;
        let raft: Arc<dyn RaftNode> = raft_impl;
        host.add_range(HostedRange {
            descriptor: descriptor.clone(),
            raft,
            space,
        })
        .await?;
    }
    let router: Arc<dyn KvRangeRouter> = Arc::new(SingleNodeServiceRangeRouter::new(descriptors));
    let executor: Arc<dyn KvRangeExecutor> = Arc::new(LocalKvRangeExecutor {
        engine: engine.clone(),
    });
    let range_runtime = Arc::new(ReplicaRuntime::with_config(
        host.clone(),
        Arc::new(NoopReplicaTransport),
        Some(Arc::new(RocksRangeLifecycleManager {
            engine: engine.clone(),
            node_id,
        })),
        Duration::from_secs(1),
        1024,
        64,
        64,
    ));
    Ok(LocalRangeRuntimeStack {
        sessiondict: Arc::new(ReplicatedSessionDictHandle::new(
            router.clone(),
            executor.clone(),
        )),
        dist: Arc::new(ReplicatedDistHandle::new(router.clone(), executor.clone())),
        inbox: Arc::new(ReplicatedInboxHandle::new(router.clone(), executor.clone())),
        retain: Arc::new(ReplicatedRetainHandle::new(router, executor)),
        range_host: host,
        range_runtime,
    })
}

async fn durable_services(
    data_dir: PathBuf,
    backend: &str,
    node_id: u64,
) -> anyhow::Result<(
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
)> {
    std::fs::create_dir_all(&data_dir)?;
    match backend {
        "sled" => {
            let session_store = Arc::new(SledSessionStore::open(data_dir.join("sessions"))?);
            let route_store = Arc::new(SledRouteStore::open(data_dir.join("routes"))?);
            let subscription_store =
                Arc::new(SledSubscriptionStore::open(data_dir.join("subscriptions"))?);
            let inbox_store = Arc::new(SledInboxStore::open(data_dir.join("inbox"))?);
            let inflight_store = Arc::new(SledInflightStore::open(data_dir.join("inflight"))?);
            let retain_store = Arc::new(SledRetainStore::open(data_dir.join("retain"))?);

            Ok((
                Arc::new(PersistentSessionDictHandle::open(session_store).await?),
                Arc::new(PersistentDistHandle::open(route_store).await?),
                Arc::new(PersistentInboxHandle::open(
                    subscription_store,
                    inbox_store,
                    inflight_store,
                )),
                Arc::new(PersistentRetainHandle::open(retain_store)),
            ))
        }
        "rocksdb" => {
            let stack = range_scoped_rocksdb_stack(data_dir, node_id).await?;
            Ok((stack.sessiondict, stack.dist, stack.inbox, stack.retain))
        }
        other => anyhow::bail!("unsupported GREENMQTT_STORAGE_BACKEND: {other}"),
    }
}

async fn redis_services(
    redis_url: &str,
) -> anyhow::Result<(
    Arc<dyn SessionDirectory>,
    Arc<dyn DistRouter>,
    Arc<dyn InboxService>,
    Arc<dyn RetainService>,
)> {
    let session_store = Arc::new(RedisSessionStore::open(redis_url)?);
    let route_store = Arc::new(RedisRouteStore::open(redis_url)?);
    let subscription_store = Arc::new(RedisSubscriptionStore::open(redis_url)?);
    let inbox_store = Arc::new(RedisInboxStore::open(redis_url)?);
    let inflight_store = Arc::new(RedisInflightStore::open(redis_url)?);
    let retain_store = Arc::new(RedisRetainStore::open(redis_url)?);

    Ok((
        Arc::new(PersistentSessionDictHandle::open(session_store).await?),
        Arc::new(PersistentDistHandle::open(route_store).await?),
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store,
            inflight_store,
        )),
        Arc::new(PersistentRetainHandle::open(retain_store)),
    ))
}

async fn replicated_range_clients(
    metadata_endpoint: &str,
    range_endpoint: &str,
) -> anyhow::Result<(Arc<dyn KvRangeRouter>, Arc<dyn KvRangeExecutor>)> {
    let metadata = MetadataGrpcClient::connect(metadata_endpoint.to_string()).await?;
    let router: Arc<dyn KvRangeRouter> = Arc::new(metadata.clone());
    let membership: Arc<dyn ClusterMembershipRegistry> = Arc::new(metadata);
    let fallback: Arc<dyn KvRangeExecutor> =
        Arc::new(KvRangeGrpcClient::connect(range_endpoint.to_string()).await?);
    let executor: Arc<dyn KvRangeExecutor> = Arc::new(LeaderRoutedKvRangeExecutor::new(
        router.clone(),
        membership,
        fallback,
        Arc::new(KvRangeGrpcExecutorFactory),
    ));
    Ok((router, executor))
}

async fn configured_retain_service(
    default_retain: Arc<dyn RetainService>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn RetainService>> {
    match resolved_state_mode("GREENMQTT_RETAIN_MODE")? {
        StateMode::Legacy => Ok(default_retain),
        StateMode::Replicated => {
            let (metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "retain")?;
            let (router, executor) =
                replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
            Ok(Arc::new(ReplicatedRetainHandle::new(router, executor)))
        }
    }
}

async fn configured_dist_service(
    default_dist: Arc<dyn DistRouter>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn DistRouter>> {
    match resolved_state_mode("GREENMQTT_DIST_MODE")? {
        StateMode::Legacy => Ok(default_dist),
        StateMode::Replicated => {
            let (metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "dist")?;
            let (router, executor) =
                replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
            Ok(Arc::new(ReplicatedDistHandle::new(router, executor)))
        }
    }
}

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            (!entry.is_empty()).then(|| entry.to_string())
        })
        .collect()
}

fn env_parse_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T> {
    match std::env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid value for {key}: {raw}")),
        _ => Ok(default),
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_millis() as u64
}

fn configured_dist_maintenance() -> anyhow::Result<Option<DistMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_DIST_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(DistMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_DIST_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        policy: ThresholdDistBalancePolicy {
            max_total_routes: env_parse_or("GREENMQTT_DIST_MAX_TOTAL_ROUTES", 5_000usize)?,
            max_wildcard_routes: env_parse_or("GREENMQTT_DIST_MAX_WILDCARD_ROUTES", 1_000usize)?,
            max_shared_routes: env_parse_or("GREENMQTT_DIST_MAX_SHARED_ROUTES", 1_000usize)?,
            desired_voters: env_parse_or("GREENMQTT_DIST_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_DIST_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_dist_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<ReplicatedDistHandle>>> {
    if resolved_state_mode("GREENMQTT_DIST_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "dist")?;
    let (router, executor) = replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
    Ok(Some(Arc::new(ReplicatedDistHandle::new(router, executor))))
}

async fn run_dist_maintenance_tick(
    dist: Arc<ReplicatedDistHandle>,
    config: &DistMaintenanceConfig,
) -> anyhow::Result<Vec<DistBalanceAction>> {
    let worker = DistMaintenanceWorker::new(dist.clone());
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = worker.refresh_tenant(tenant_id).await?;
        let routes = dist.list_routes(Some(tenant_id)).await?;
        stats.push(DistTenantStats {
            tenant_id: tenant_id.clone(),
            total_routes: routes.len(),
            wildcard_routes: routes
                .iter()
                .filter(|route| {
                    route.topic_filter.contains('#') || route.topic_filter.contains('+')
                })
                .count(),
            shared_routes: routes
                .iter()
                .filter(|route| route.shared_group.is_some())
                .count(),
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_dist_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    dist: Arc<ReplicatedDistHandle>,
    config: DistMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_dist_maintenance_tick(dist.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!(
                    "greenmqtt dist maintenance proposed actions: {:?}",
                    actions
                );
            }
        }
    });
}

fn configured_inbox_maintenance() -> anyhow::Result<Option<InboxMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_INBOX_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(InboxMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_INBOX_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        retry_delay: Duration::from_millis(env_parse_or(
            "GREENMQTT_INBOX_MAINTENANCE_RETRY_DELAY_MS",
            100u64,
        )?),
        max_retries: env_parse_or("GREENMQTT_INBOX_MAINTENANCE_MAX_RETRIES", 2usize)?,
        policy: ThresholdInboxBalancePolicy {
            max_subscriptions: env_parse_or(
                "GREENMQTT_INBOX_MAX_SUBSCRIPTIONS",
                5_000usize,
            )?,
            max_offline_messages: env_parse_or(
                "GREENMQTT_INBOX_MAX_OFFLINE_MESSAGES",
                20_000usize,
            )?,
            max_inflight_messages: env_parse_or(
                "GREENMQTT_INBOX_MAX_INFLIGHT_MESSAGES",
                10_000usize,
            )?,
            desired_voters: env_parse_or("GREENMQTT_INBOX_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_INBOX_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_inbox_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<ReplicatedInboxHandle>>> {
    if resolved_state_mode("GREENMQTT_INBOX_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "inbox")?;
    let (router, executor) = replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
    Ok(Some(Arc::new(ReplicatedInboxHandle::new(router, executor))))
}

async fn run_inbox_maintenance_tick(
    inbox: Arc<ReplicatedInboxHandle>,
    config: &InboxMaintenanceConfig,
) -> anyhow::Result<Vec<InboxBalanceAction>> {
    let handler = Arc::new(InboxServiceDelayHandler::new(
        inbox.clone(),
        Arc::new(NoopDelayedLwtPublisher),
    ));
    let worker = InboxMaintenanceWorker::new(
        TenantInboxGcRunner::new(inbox.clone()),
        InboxDelayTaskRunner::new(handler, config.max_retries, config.retry_delay),
    );
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let task = worker.schedule_delayed_task(
            InboxDelayedTask::ExpireTenant {
                tenant_id: tenant_id.clone(),
                now_ms: current_time_ms(),
            },
            Duration::ZERO,
        );
        task.await??;
        stats.push(InboxTenantStats {
            tenant_id: tenant_id.clone(),
            subscriptions: inbox.count_tenant_subscriptions(tenant_id).await?,
            offline_messages: inbox.count_tenant_offline(tenant_id).await?,
            inflight_messages: inbox.count_tenant_inflight(tenant_id).await?,
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_inbox_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    inbox: Arc<ReplicatedInboxHandle>,
    config: InboxMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_inbox_maintenance_tick(inbox.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!(
                    "greenmqtt inbox maintenance proposed actions: {:?}",
                    actions
                );
            }
        }
    });
}

fn configured_retain_maintenance() -> anyhow::Result<Option<RetainMaintenanceConfig>> {
    let tenants = std::env::var("GREENMQTT_RETAIN_MAINTENANCE_TENANTS")
        .ok()
        .map(|raw| parse_csv_list(&raw))
        .unwrap_or_default();
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(RetainMaintenanceConfig {
        tenants,
        interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_RETAIN_MAINTENANCE_INTERVAL_SECS",
            30u64,
        )?),
        policy: ThresholdRetainBalancePolicy {
            max_retained_messages: env_parse_or(
                "GREENMQTT_RETAIN_MAX_MESSAGES",
                10_000usize,
            )?,
            desired_voters: env_parse_or("GREENMQTT_RETAIN_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_RETAIN_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

async fn replicated_retain_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<ReplicatedRetainHandle>>> {
    if resolved_state_mode("GREENMQTT_RETAIN_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "retain")?;
    let (router, executor) = replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
    Ok(Some(Arc::new(ReplicatedRetainHandle::new(router, executor))))
}

async fn run_retain_maintenance_tick(
    retain: Arc<ReplicatedRetainHandle>,
    config: &RetainMaintenanceConfig,
) -> anyhow::Result<Vec<RetainBalanceAction>> {
    let worker = RetainMaintenanceWorker::new(retain.clone());
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = worker.refresh_tenant(tenant_id).await?;
        stats.push(RetainTenantStats {
            tenant_id: tenant_id.clone(),
            retained_messages: retain.list_tenant_retained(tenant_id).await?.len(),
        });
    }
    Ok(config.policy.propose(&stats))
}

fn spawn_retain_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    retain: Arc<ReplicatedRetainHandle>,
    config: RetainMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_retain_maintenance_tick(retain.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!(
                    "greenmqtt retain maintenance proposed actions: {:?}",
                    actions
                );
            }
        }
    });
}

async fn configured_sessiondict_service(
    default_sessiondict: Arc<dyn SessionDirectory>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn SessionDirectory>> {
    match resolved_state_mode("GREENMQTT_SESSIONDICT_MODE")? {
        StateMode::Legacy => Ok(default_sessiondict),
        StateMode::Replicated => {
            let (metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "sessiondict")?;
            let (router, executor) =
                replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
            Ok(Arc::new(ReplicatedSessionDictHandle::new(router, executor)))
        }
    }
}

async fn configured_inbox_service(
    default_inbox: Arc<dyn InboxService>,
    state_endpoint: Option<&str>,
) -> anyhow::Result<Arc<dyn InboxService>> {
    match resolved_state_mode("GREENMQTT_INBOX_MODE")? {
        StateMode::Legacy => Ok(default_inbox),
        StateMode::Replicated => {
            let (metadata_endpoint, range_endpoint) =
                resolved_replicated_endpoints(state_endpoint, "inbox")?;
            let (router, executor) =
                replicated_range_clients(&metadata_endpoint, &range_endpoint).await?;
            Ok(Arc::new(ReplicatedInboxHandle::new(router, executor)))
        }
    }
}

fn compare_data_dir(backend: &str) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    std::env::temp_dir().join(format!("greenmqtt-compare-{backend}-{timestamp}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateMode {
    Legacy,
    Replicated,
}

#[derive(Debug, Clone)]
struct DistMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    policy: ThresholdDistBalancePolicy,
}

#[derive(Debug, Clone)]
struct InboxMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    retry_delay: Duration,
    max_retries: usize,
    policy: ThresholdInboxBalancePolicy,
}

#[derive(Debug, Clone)]
struct RetainMaintenanceConfig {
    tenants: Vec<String>,
    interval: Duration,
    policy: ThresholdRetainBalancePolicy,
}

#[derive(Clone, Default)]
struct NoopDelayedLwtPublisher;

#[async_trait]
impl DelayedLwtSink for NoopDelayedLwtPublisher {
    async fn send_lwt(&self, _publish: &DelayedLwtPublish) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct RocksRangeLifecycleManager {
    engine: Arc<RocksDbKvEngine>,
    node_id: u64,
}

#[async_trait]
impl RangeLifecycleManager for RocksRangeLifecycleManager {
    async fn create_range(
        &self,
        descriptor: ReplicatedRangeDescriptor,
    ) -> anyhow::Result<HostedRange> {
        self.engine
            .bootstrap(KvRangeBootstrap {
                range_id: descriptor.id.clone(),
                boundary: descriptor.boundary.clone(),
            })
            .await?;
        let space = Arc::from(self.engine.open_range(&descriptor.id).await?);
        let raft_impl = Arc::new(MemoryRaftNode::single_node(self.node_id, &descriptor.id));
        raft_impl.recover().await?;
        let raft: Arc<dyn RaftNode> = raft_impl;
        Ok(HostedRange {
            descriptor,
            raft,
            space,
        })
    }

    async fn retire_range(&self, range_id: &str) -> anyhow::Result<()> {
        self.engine.destroy_range(range_id).await
    }
}

fn parse_state_mode(value: &str) -> anyhow::Result<StateMode> {
    match value {
        "legacy" => Ok(StateMode::Legacy),
        "replicated" => Ok(StateMode::Replicated),
        other => anyhow::bail!("unsupported state mode: {other}"),
    }
}

fn resolved_state_mode(service_env_var: &str) -> anyhow::Result<StateMode> {
    if let Some(value) = std::env::var(service_env_var)
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        return parse_state_mode(&value.to_lowercase())
            .map_err(|_| anyhow::anyhow!("unsupported {service_env_var}: {value}"));
    }
    let global = std::env::var("GREENMQTT_STATE_MODE")
        .unwrap_or_else(|_| "legacy".to_string())
        .to_lowercase();
    parse_state_mode(&global)
        .map_err(|_| anyhow::anyhow!("unsupported GREENMQTT_STATE_MODE: {global}"))
}

fn resolved_replicated_endpoints(
    state_endpoint: Option<&str>,
    service_name: &str,
) -> anyhow::Result<(String, String)> {
    let metadata_endpoint = std::env::var("GREENMQTT_METADATA_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(ToString::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "replicated {service_name} mode requires GREENMQTT_METADATA_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
            )
        })?;
    let range_endpoint = std::env::var("GREENMQTT_RANGE_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| state_endpoint.map(ToString::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "replicated {service_name} mode requires GREENMQTT_RANGE_ENDPOINT or GREENMQTT_STATE_ENDPOINT"
            )
        })?;
    Ok((metadata_endpoint, range_endpoint))
}

async fn compare_bench_broker(
    node_id: u64,
    backend: &str,
    redis_url: &str,
) -> anyhow::Result<(AppBroker, Option<PathBuf>)> {
    let (default_auth, default_acl, default_hooks) = default_listener_profiles(
        ConfiguredAuth::default(),
        ConfiguredAcl::default(),
        ConfiguredEventHook::default(),
    );
    let (sessiondict, dist, inbox, retain, cleanup_dir): CompareStateServices = match backend {
        "memory" => (
            Arc::new(SessionDictHandle::default()),
            Arc::new(DistHandle::default()),
            Arc::new(InboxHandle::default()),
            Arc::new(RetainHandle::default()),
            None,
        ),
        "sled" | "rocksdb" => {
            let path = compare_data_dir(backend);
            let (sessiondict, dist, inbox, retain) =
                durable_services(path.clone(), backend, node_id).await?;
            (sessiondict, dist, inbox, retain, Some(path))
        }
        "redis" => {
            let (sessiondict, dist, inbox, retain) = redis_services(redis_url).await?;
            (sessiondict, dist, inbox, retain, None)
        }
        other => anyhow::bail!("unsupported GREENMQTT_COMPARE_BACKENDS value: {other}"),
    };
    Ok((
        Arc::new(BrokerRuntime::with_plugins(
            BrokerConfig {
                node_id,
                enable_tcp: true,
                enable_tls: false,
                enable_ws: false,
                enable_wss: false,
                enable_quic: false,
                server_keep_alive_secs: None,
                max_packet_size: None,
                response_information: None,
                server_reference: None,
                audit_log_path: cleanup_dir
                    .as_ref()
                    .map(|path| path.join("admin-audit.jsonl")),
            },
            default_auth,
            default_acl,
            default_hooks,
            sessiondict,
            dist,
            inbox,
            retain,
        )),
        cleanup_dir,
    ))
}

#[cfg(test)]
#[cfg(test)]
mod main_tests;
