use crate::*;

fn parse_csv_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .filter_map(|entry| {
            let entry = entry.trim();
            (!entry.is_empty()).then(|| entry.to_string())
        })
        .collect()
}

pub(crate) fn env_parse_or<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T> {
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

fn env_parse_bool(key: &str, default: bool) -> anyhow::Result<bool> {
    match std::env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => anyhow::bail!("invalid value for {key}: {raw}"),
        },
        _ => Ok(default),
    }
}

pub(crate) async fn configured_control_plane_runtime(
    node_id: u64,
    registry: Arc<dyn ControlPlaneRegistry>,
    metadata_endpoint: String,
    range_endpoint: String,
) -> anyhow::Result<Option<ControlPlaneRuntime>> {
    if !env_parse_bool("GREENMQTT_CONTROL_PLANE_ENABLED", false)? {
        return Ok(None);
    }
    let controller_node_id =
        env_parse_or::<u64>("GREENMQTT_CONTROL_PLANE_CONTROLLER_NODE_ID", node_id)?;
    if controller_node_id != node_id {
        eprintln!(
            "greenmqtt control plane runtime not started on node {node_id}; controller node is {controller_node_id}"
        );
        return Ok(None);
    }
    let desired_ranges = std::env::var("GREENMQTT_CONTROL_PLANE_DESIRED_RANGES_JSON")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|raw| {
            serde_json::from_str::<Vec<ReplicatedRangeDescriptor>>(&raw).map_err(|error| {
                anyhow::anyhow!(
                    "invalid GREENMQTT_CONTROL_PLANE_DESIRED_RANGES_JSON payload: {error}"
                )
            })
        })
        .transpose()?
        .unwrap_or_default();
    let config = ControlPlaneRuntimeConfig {
        balancer_name: std::env::var("GREENMQTT_CONTROL_PLANE_BALANCER_NAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "metadata-control-plane".to_string()),
        controller_id: std::env::var("GREENMQTT_CONTROL_PLANE_CONTROLLER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("node-{node_id}")),
        desired_ranges,
        voters_per_range: env_parse_or("GREENMQTT_CONTROL_PLANE_VOTERS_PER_RANGE", 3usize)?,
        learners_per_range: env_parse_or("GREENMQTT_CONTROL_PLANE_LEARNERS_PER_RANGE", 0usize)?,
        bootstrap_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_BOOTSTRAP_INTERVAL_SECS",
            30u64,
        )?),
        replica_reconcile_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_REPLICA_INTERVAL_SECS",
            30u64,
        )?),
        leader_rebalance_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_LEADER_INTERVAL_SECS",
            30u64,
        )?),
        cleanup_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_CLEANUP_INTERVAL_SECS",
            30u64,
        )?),
        recovery_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_RECOVERY_INTERVAL_SECS",
            30u64,
        )?),
        command_reconcile_interval: Duration::from_secs(env_parse_or(
            "GREENMQTT_CONTROL_PLANE_COMMAND_INTERVAL_SECS",
            10u64,
        )?),
    };
    let controller = Arc::new(MetadataBalanceController::new(registry.clone()));
    let coordinator = BalanceCoordinator::with_reconciliation(
        controller.clone(),
        Arc::new(ControlPlaneExecutor {
            registry: registry.clone(),
            range_control: RoutedRangeControlGrpcClient::connect(metadata_endpoint, range_endpoint)
                .await?,
        }),
        registry.clone(),
        config.balancer_name.clone(),
    );
    Ok(Some(ControlPlaneRuntime::new(
        coordinator,
        registry,
        config,
    )))
}

pub(crate) fn configured_dist_maintenance() -> anyhow::Result<Option<DistMaintenanceConfig>> {
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

pub(crate) async fn replicated_dist_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn DistRuntime>>> {
    if resolved_state_mode("GREENMQTT_DIST_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "dist")?;
    Ok(Some(Arc::new(
        DistGrpcClient::connect(range_endpoint).await?,
    )))
}

pub(crate) async fn run_dist_maintenance_tick(
    dist: Arc<dyn DistRuntime>,
    config: &DistMaintenanceConfig,
) -> anyhow::Result<Vec<DistBalanceAction>> {
    let worker = DistMaintenanceWorker::new(dist.clone());
    let tenants = if config.targets_all_tenants() {
        dist.list_routes(None)
            .await?
            .into_iter()
            .map(|route| route.tenant_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        config.tenants.clone()
    };
    if tenants.is_empty() {
        return Ok(Vec::new());
    }
    let mut stats = Vec::new();
    for tenant_id in &tenants {
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

pub(crate) fn spawn_dist_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    dist: Arc<dyn DistRuntime>,
    registry: Arc<dyn ControlPlaneRegistry>,
    range_control: RoutedRangeControlGrpcClient,
    config: DistMaintenanceConfig,
) {
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await;
            let actions = run_dist_maintenance_tick(dist.clone(), &config).await?;
            if !actions.is_empty() {
                eprintln!("greenmqtt dist maintenance proposed actions: {:?}", actions);
                for action in &actions {
                    if let DistBalanceAction::SplitTenantRange { tenant_id } = action {
                        let routes = dist.list_routes(Some(tenant_id)).await?;
                        if routes.len() < 2 {
                            continue;
                        }
                        let mut ranges = registry.list_ranges(Some(ServiceShardKind::Dist)).await?;
                        ranges.retain(|range| {
                            range.shard.tenant_id == *tenant_id || range.shard.tenant_id == "*"
                        });
                        let mut selected = None;
                        for range in ranges {
                            let mut range_topic_filters = routes
                                .iter()
                                .map(|route| route.topic_filter.clone())
                                .filter(|topic_filter| range.contains_key(topic_filter.as_bytes()))
                                .collect::<Vec<_>>();
                            if range_topic_filters.len() < 2 {
                                continue;
                            }
                            range_topic_filters.sort();
                            let split_index = range_topic_filters.len() / 2;
                            let split_key = range_topic_filters[split_index].as_bytes().to_vec();
                            if range.boundary.start_key.as_deref() == Some(split_key.as_slice()) {
                                continue;
                            }
                            selected = Some((range.id.clone(), split_key));
                            break;
                        }
                        let Some((range_id, split_key)) = selected else {
                            continue;
                        };
                        match range_control
                            .split_range(&range_id, split_key.clone())
                            .await
                        {
                            Ok((left_range_id, right_range_id)) => eprintln!(
                                "greenmqtt dist maintenance split range_id={} left={} right={} split_key={:?}",
                                range_id,
                                left_range_id,
                                right_range_id,
                                split_key
                            ),
                            Err(error) => eprintln!(
                                "greenmqtt dist maintenance split failed range_id={} split_key={:?} error={error}",
                                range_id,
                                split_key
                            ),
                        }
                    }
                }
            }
        }
    });
}

pub(crate) fn configured_inbox_maintenance() -> anyhow::Result<Option<InboxMaintenanceConfig>> {
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
        policy: ThresholdInboxBalancePolicy {
            max_subscriptions: env_parse_or("GREENMQTT_INBOX_MAX_SUBSCRIPTIONS", 5_000usize)?,
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

pub(crate) async fn replicated_inbox_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn InboxService>>> {
    if resolved_state_mode("GREENMQTT_INBOX_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "inbox")?;
    Ok(Some(Arc::new(
        InboxGrpcClient::connect(range_endpoint).await?,
    )))
}

pub(crate) async fn run_inbox_maintenance_tick(
    inbox: Arc<dyn InboxService>,
    config: &InboxMaintenanceConfig,
) -> anyhow::Result<Vec<InboxBalanceAction>> {
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = inbox_tenant_gc_run(inbox.as_ref(), tenant_id, current_time_ms()).await?;
        stats.push(InboxTenantStats {
            tenant_id: tenant_id.clone(),
            subscriptions: inbox.count_tenant_subscriptions(tenant_id).await?,
            offline_messages: inbox.count_tenant_offline(tenant_id).await?,
            inflight_messages: inbox.count_tenant_inflight(tenant_id).await?,
        });
    }
    Ok(config.policy.propose(&stats))
}

pub(crate) fn spawn_inbox_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    inbox: Arc<dyn InboxService>,
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

pub(crate) fn configured_retain_maintenance() -> anyhow::Result<Option<RetainMaintenanceConfig>> {
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
            max_retained_messages: env_parse_or("GREENMQTT_RETAIN_MAX_MESSAGES", 10_000usize)?,
            desired_voters: env_parse_or("GREENMQTT_RETAIN_DESIRED_VOTERS", 3usize)?,
            desired_learners: env_parse_or("GREENMQTT_RETAIN_DESIRED_LEARNERS", 1usize)?,
        },
    }))
}

pub(crate) async fn replicated_retain_runtime_handle(
    state_endpoint: Option<&str>,
) -> anyhow::Result<Option<Arc<dyn RetainService>>> {
    if resolved_state_mode("GREENMQTT_RETAIN_MODE")? != StateMode::Replicated {
        return Ok(None);
    }
    let (_metadata_endpoint, range_endpoint) =
        resolved_replicated_endpoints(state_endpoint, "retain")?;
    Ok(Some(Arc::new(
        RetainGrpcClient::connect(range_endpoint).await?,
    )))
}

pub(crate) async fn run_retain_maintenance_tick(
    retain: Arc<dyn RetainService>,
    config: &RetainMaintenanceConfig,
) -> anyhow::Result<Vec<RetainBalanceAction>> {
    let mut stats = Vec::new();
    for tenant_id in &config.tenants {
        let _ = retain_tenant_gc_run(retain.as_ref(), tenant_id).await?;
        stats.push(RetainTenantStats {
            tenant_id: tenant_id.clone(),
            retained_messages: retain.list_tenant_retained(tenant_id).await?.len(),
        });
    }
    Ok(config.policy.propose(&stats))
}

pub(crate) fn spawn_retain_maintenance_task(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    retain: Arc<dyn RetainService>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateMode {
    Legacy,
    Replicated,
}

#[derive(Debug, Clone)]
pub(crate) struct DistMaintenanceConfig {
    pub(crate) tenants: Vec<String>,
    pub(crate) interval: Duration,
    pub(crate) policy: ThresholdDistBalancePolicy,
}

impl DistMaintenanceConfig {
    pub(crate) fn targets_all_tenants(&self) -> bool {
        self.tenants
            .iter()
            .any(|tenant| tenant == "*" || tenant.eq_ignore_ascii_case("all"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InboxMaintenanceConfig {
    pub(crate) tenants: Vec<String>,
    pub(crate) interval: Duration,
    pub(crate) policy: ThresholdInboxBalancePolicy,
}

#[derive(Debug, Clone)]
pub(crate) struct RetainMaintenanceConfig {
    pub(crate) tenants: Vec<String>,
    pub(crate) interval: Duration,
    pub(crate) policy: ThresholdRetainBalancePolicy,
}

#[derive(Clone)]
pub(crate) struct ControlPlaneExecutor {
    pub(crate) registry: Arc<dyn ControlPlaneRegistry>,
    pub(crate) range_control: RoutedRangeControlGrpcClient,
}

#[async_trait]
impl BalanceCommandExecutor for ControlPlaneExecutor {
    async fn execute(&self, command: &BalanceCommand) -> anyhow::Result<bool> {
        match command {
            BalanceCommand::BootstrapRange { range_id, .. } => {
                let descriptor = self
                    .registry
                    .resolve_range(range_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bootstrap range missing from metadata"))?;
                let _ = self.range_control.bootstrap_range(descriptor).await?;
                Ok(true)
            }
            BalanceCommand::ChangeReplicas {
                range_id,
                voters,
                learners,
            } => {
                self.range_control
                    .change_replicas(range_id, voters.clone(), learners.clone())
                    .await?;
                Ok(true)
            }
            BalanceCommand::TransferLeadership {
                range_id,
                to_node_id,
                ..
            } => {
                self.range_control
                    .transfer_leadership(range_id, *to_node_id)
                    .await?;
                Ok(true)
            }
            BalanceCommand::RecoverRange {
                range_id,
                new_leader_node_id,
            } => {
                self.range_control
                    .recover_range(range_id, *new_leader_node_id)
                    .await?;
                Ok(true)
            }
            BalanceCommand::CleanupReplicas { .. }
            | BalanceCommand::MigrateShard { .. }
            | BalanceCommand::FailoverShard { .. }
            | BalanceCommand::RecordBalancerState { .. }
            | BalanceCommand::SplitRange { .. }
            | BalanceCommand::MergeRanges { .. } => Ok(true),
        }
    }
}

pub(crate) fn parse_state_mode(value: &str) -> anyhow::Result<StateMode> {
    match value {
        "legacy" => Ok(StateMode::Legacy),
        "replicated" => Ok(StateMode::Replicated),
        other => anyhow::bail!("unsupported state mode: {other}"),
    }
}

pub(crate) fn resolved_state_mode(service_env_var: &str) -> anyhow::Result<StateMode> {
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

pub(crate) fn resolved_replicated_endpoints(
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
    Ok((
        normalize_service_endpoint(&metadata_endpoint, ServiceEndpointTransport::GrpcHttp)?,
        normalize_service_endpoint(&range_endpoint, ServiceEndpointTransport::GrpcHttp)?,
    ))
}
