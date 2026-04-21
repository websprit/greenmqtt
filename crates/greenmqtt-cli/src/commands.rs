use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ShardActionEnvelope {
    previous: Option<greenmqtt_core::ServiceShardAssignment>,
    current: Option<greenmqtt_core::ServiceShardAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RangeCliCommand {
    Inspect {
        range_id: String,
    },
    Bootstrap {
        descriptor: ReplicatedRangeDescriptor,
    },
    Split {
        target: RangeCliTarget,
        split_key: Vec<u8>,
    },
    Merge {
        left_range_id: String,
        right_range_id: String,
    },
    Drain {
        target: RangeCliTarget,
    },
    Retire {
        target: RangeCliTarget,
    },
    Recover {
        target: RangeCliTarget,
        new_leader_node_id: u64,
    },
    ChangeReplicas {
        target: RangeCliTarget,
        voters: Vec<u64>,
        learners: Vec<u64>,
    },
    TransferLeadership {
        target: RangeCliTarget,
        target_node_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RangeCliTarget {
    RangeId(String),
    Shard(ServiceShardKey),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RangeCliReply {
    operation: String,
    ok: bool,
    status: String,
    reason: String,
    mode: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    forwarded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_node_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    left_range_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    right_range_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedRangeExecution {
    range_id: String,
    forwarded: bool,
    target_node_id: Option<u64>,
    target_endpoint: Option<String>,
}

pub(crate) fn run_shard_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
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

pub(crate) fn run_control_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let addr = std::env::var("GREENMQTT_HTTP_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
        .parse::<SocketAddr>()?;
    let mut args = args;
    let subcommand = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing control-command subcommand"))?;
    let (method, path, body) = match subcommand.as_str() {
        "list" => {
            let mut query = Vec::new();
            while let Some(flag) = args.next() {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing value for {flag}"))?;
                match flag.as_str() {
                    "--execution-state" => query.push(format!("execution_state={value}")),
                    "--reflection-state" => query.push(format!("reflection_state={value}")),
                    "--issued-by" => query.push(format!("issued_by={value}")),
                    "--target" => query.push(format!("target_range_or_shard={value}")),
                    _ => anyhow::bail!("unknown control-command flag: {flag}"),
                }
            }
            let path = if query.is_empty() {
                "/v1/control-commands".to_string()
            } else {
                format!("/v1/control-commands?{}", query.join("&"))
            };
            ("GET".to_string(), path, None)
        }
        "get" => {
            let command_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing command_id"))?;
            (
                "GET".to_string(),
                format!("/v1/control-commands/{command_id}"),
                None,
            )
        }
        "retry" => {
            let command_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing command_id"))?;
            (
                "POST".to_string(),
                format!("/v1/control-commands/{command_id}/retry"),
                Some("{}".to_string()),
            )
        }
        "fail" => {
            let command_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing command_id"))?;
            let last_error = args.next();
            (
                "POST".to_string(),
                format!("/v1/control-commands/{command_id}/fail"),
                Some(
                    serde_json::json!({
                        "last_error": last_error,
                    })
                    .to_string(),
                ),
            )
        }
        "prune" => {
            let older_than_ms = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing older_than_ms"))?;
            (
                "DELETE".to_string(),
                format!("/v1/control-commands?older_than_ms={older_than_ms}"),
                None,
            )
        }
        other => anyhow::bail!("unknown control-command subcommand: {other}"),
    };
    println!(
        "{}",
        http_request_body(addr, &method, &path, body.as_deref())?
    );
    Ok(())
}

pub(crate) async fn run_range_command(args: impl Iterator<Item = String>) -> anyhow::Result<()> {
    let endpoint = range_control_endpoint()?;
    let response = execute_range_command_with_endpoint(args, &endpoint).await?;
    println!("{response}");
    Ok(())
}

pub(crate) async fn execute_range_command_with_endpoint(
    args: impl Iterator<Item = String>,
    endpoint: &str,
) -> anyhow::Result<String> {
    let (command, output_mode) = range_command_request(args)?;
    if let RangeCliCommand::Inspect { range_id } = command {
        let addr = std::env::var("GREENMQTT_HTTP_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse::<SocketAddr>()?;
        let response = http_request_body(addr, "GET", &format!("/v1/ranges/{range_id}"), None)?;
        return match output_mode {
            OutputMode::Json => Ok(response),
            OutputMode::Text => Ok(render_range_lookup_text(&response)?),
        };
    }
    let client = RangeControlGrpcClient::connect(endpoint.to_string()).await?;
    let routed_client = RoutedRangeControlGrpcClient::connect(
        range_metadata_endpoint(endpoint)?,
        endpoint.to_string(),
    )
    .await?;
    let reply = match command {
        RangeCliCommand::Bootstrap { descriptor } => RangeCliReply {
            operation: "bootstrap".into(),
            ok: true,
            status: "bootstrapped".into(),
            reason: "Range bootstrapped and marked serving.".into(),
            mode: "local".into(),
            forwarded: false,
            target_node_id: None,
            target_endpoint: None,
            command_id: None,
            audit_action: None,
            range_id: Some(client.bootstrap_range(descriptor).await?),
            left_range_id: None,
            right_range_id: None,
        },
        RangeCliCommand::Split { target, split_key } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            let (left_range_id, right_range_id) = if resolved.forwarded {
                routed_client
                    .split_range(&resolved.range_id, split_key)
                    .await?
            } else {
                client.split_range(&resolved.range_id, split_key).await?
            };
            RangeCliReply {
                operation: "split".into(),
                ok: true,
                status: "split".into(),
                reason: "Range split completed; child ranges are now available.".into(),
                mode: if resolved.forwarded {
                    "forwarded".into()
                } else {
                    "local".into()
                },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
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
            mode: "local".into(),
            forwarded: false,
            target_node_id: None,
            target_endpoint: None,
            command_id: None,
            audit_action: None,
            range_id: Some(client.merge_ranges(&left_range_id, &right_range_id).await?),
            left_range_id: Some(left_range_id),
            right_range_id: Some(right_range_id),
        },
        RangeCliCommand::Drain { target } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            if resolved.forwarded {
                routed_client.drain_range(&resolved.range_id).await?;
            } else {
                client.drain_range(&resolved.range_id).await?;
            }
            RangeCliReply {
                operation: "drain".into(),
                ok: true,
                status: "draining".into(),
                reason: "Range marked draining; writes should migrate away before retirement."
                    .into(),
                mode: if resolved.forwarded {
                    "forwarded".into()
                } else {
                    "local".into()
                },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::Retire { target } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            if resolved.forwarded {
                routed_client.retire_range(&resolved.range_id).await?;
            } else {
                client.retire_range(&resolved.range_id).await?;
            }
            RangeCliReply {
                operation: "retire".into(),
                ok: true,
                status: "retired".into(),
                reason: "Range retired; repeated retire requests are treated as already applied."
                    .into(),
                mode: if resolved.forwarded {
                    "forwarded".into()
                } else {
                    "local".into()
                },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::Recover {
            target,
            new_leader_node_id,
        } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            if resolved.forwarded {
                routed_client
                    .recover_range(&resolved.range_id, new_leader_node_id)
                    .await?;
            } else {
                client
                    .recover_range(&resolved.range_id, new_leader_node_id)
                    .await?;
            }
            RangeCliReply {
                operation: "recover".into(),
                ok: true,
                status: "recovered".into(),
                reason: "Recovery command accepted; the target leader is now authoritative or already was.".into(),
                mode: if resolved.forwarded { "forwarded".into() } else { "local".into() },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::ChangeReplicas {
            target,
            voters,
            learners,
        } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            if resolved.forwarded {
                routed_client
                    .change_replicas(&resolved.range_id, voters, learners)
                    .await?;
            } else {
                client
                    .change_replicas(&resolved.range_id, voters, learners)
                    .await?;
            }
            RangeCliReply {
                operation: "change-replicas".into(),
                ok: true,
                status: "reconfiguring".into(),
                reason: "Replica change accepted; consult range health for staging or catch-up progress.".into(),
                mode: if resolved.forwarded { "forwarded".into() } else { "local".into() },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::TransferLeadership {
            target,
            target_node_id,
        } => {
            let resolved = resolve_range_execution(&target, endpoint, &routed_client).await?;
            if resolved.forwarded {
                routed_client
                    .transfer_leadership(&resolved.range_id, target_node_id)
                    .await?;
            } else {
                client
                    .transfer_leadership(&resolved.range_id, target_node_id)
                    .await?;
            }
            RangeCliReply {
                operation: "transfer-leadership".into(),
                ok: true,
                status: "leadership-updated".into(),
                reason: "Leadership transfer accepted; if the target already led the range this was a no-op.".into(),
                mode: if resolved.forwarded { "forwarded".into() } else { "local".into() },
                forwarded: resolved.forwarded,
                target_node_id: resolved.target_node_id,
                target_endpoint: resolved.target_endpoint,
                command_id: None,
                audit_action: None,
                range_id: Some(resolved.range_id),
                left_range_id: None,
                right_range_id: None,
            }
        }
        RangeCliCommand::Inspect { .. } => unreachable!("inspect is handled before gRPC execution"),
    };
    match output_mode {
        OutputMode::Json => Ok(serde_json::to_string(&reply)?),
        OutputMode::Text => Ok(render_range_response_text(&reply)),
    }
}

pub(crate) fn range_control_endpoint() -> anyhow::Result<String> {
    let raw = std::env::var("GREENMQTT_RANGE_CONTROL_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("GREENMQTT_RPC_BIND")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| "127.0.0.1:50051".to_string());
    Ok(
        if raw.starts_with("http://") || raw.starts_with("https://") {
            raw
        } else {
            format!("http://{raw}")
        },
    )
}

pub(crate) fn range_metadata_endpoint(default_endpoint: &str) -> anyhow::Result<String> {
    let raw = std::env::var("GREENMQTT_METADATA_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_endpoint.to_string());
    Ok(
        if raw.starts_with("http://") || raw.starts_with("https://") {
            raw
        } else {
            format!("http://{raw}")
        },
    )
}

fn finish_range_target(
    range_id: Option<String>,
    kind: Option<ServiceShardKind>,
    tenant_id: Option<String>,
    scope: Option<String>,
) -> anyhow::Result<RangeCliTarget> {
    if let Some(range_id) = range_id {
        return Ok(RangeCliTarget::RangeId(range_id));
    }
    Ok(RangeCliTarget::Shard(ServiceShardKey {
        kind: kind.ok_or_else(|| anyhow::anyhow!("missing --kind when range_id is omitted"))?,
        tenant_id: tenant_id
            .ok_or_else(|| anyhow::anyhow!("missing --tenant-id when range_id is omitted"))?,
        scope: scope.ok_or_else(|| anyhow::anyhow!("missing --scope when range_id is omitted"))?,
    }))
}

async fn resolve_range_execution(
    target: &RangeCliTarget,
    requested_endpoint: &str,
    client: &RoutedRangeControlGrpcClient,
) -> anyhow::Result<ResolvedRangeExecution> {
    match target {
        RangeCliTarget::RangeId(range_id) => {
            if let Ok(target) = client.resolve_range_target(range_id).await {
                let forwarded = target.endpoint != requested_endpoint;
                return Ok(ResolvedRangeExecution {
                    range_id: target.range_id,
                    forwarded,
                    target_node_id: forwarded.then_some(target.node_id),
                    target_endpoint: forwarded.then_some(target.endpoint),
                });
            }
            Ok(ResolvedRangeExecution {
                range_id: range_id.clone(),
                forwarded: false,
                target_node_id: None,
                target_endpoint: None,
            })
        }
        RangeCliTarget::Shard(shard) => {
            let target = client.resolve_shard_target(shard).await?;
            let forwarded = target.endpoint != requested_endpoint;
            Ok(ResolvedRangeExecution {
                range_id: target.range_id,
                forwarded,
                target_node_id: forwarded.then_some(target.node_id),
                target_endpoint: forwarded.then_some(target.endpoint),
            })
        }
    }
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
    let base = match (
        reply.range_id.as_deref(),
        reply.left_range_id.as_deref(),
        reply.right_range_id.as_deref(),
    ) {
        (Some(range_id), Some(left), Some(right)) => {
            format!(
                "ok operation={} mode={} status={} reason=\"{}\" range_id={} left_range_id={} right_range_id={}",
                reply.operation, reply.mode, reply.status, reply.reason, range_id, left, right
            )
        }
        (Some(range_id), _, _) => format!(
            "ok operation={} mode={} status={} reason=\"{}\" range_id={}",
            reply.operation, reply.mode, reply.status, reply.reason, range_id
        ),
        _ => format!(
            "ok operation={} mode={} status={} reason=\"{}\"",
            reply.operation, reply.mode, reply.status, reply.reason
        ),
    };
    if reply.forwarded {
        format!(
            "{} forwarded=true target_node_id={} target_endpoint={}",
            base,
            reply.target_node_id.unwrap_or_default(),
            reply.target_endpoint.as_deref().unwrap_or_default()
        )
    } else {
        base
    }
}

pub(crate) fn render_range_lookup_text(body: &str) -> anyhow::Result<String> {
    let value: serde_json::Value = serde_json::from_str(body)?;
    let source = value
        .get("source")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let range = value
        .get("health")
        .and_then(|value| value.get("range_id"))
        .or_else(|| value.get("descriptor").and_then(|value| value.get("id")))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    Ok(format!(
        "ok operation=inspect source={source} range_id={range}"
    ))
}

pub(crate) fn range_command_request(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<(RangeCliCommand, OutputMode)> {
    let subcommand = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing range subcommand"))?;
    match subcommand.as_str() {
        "inspect" => {
            let range_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range_id"))?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range inspect flag: {flag}"),
                }
            }
            Ok((RangeCliCommand::Inspect { range_id }, output))
        }
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
                    "--range-id" => {
                        range_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --range-id"))?,
                        )
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--start-key" => {
                        start_key = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --start-key"))?
                                .into_bytes(),
                        )
                    }
                    "--end-key" => {
                        end_key = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --end-key"))?
                                .into_bytes(),
                        )
                    }
                    "--epoch" => {
                        epoch = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --epoch"))?
                            .parse()?
                    }
                    "--config-version" => {
                        config_version = args
                            .next()
                            .ok_or_else(|| anyhow::anyhow!("missing value for --config-version"))?
                            .parse()?
                    }
                    "--leader-node-id" => {
                        leader_node_id = Some(
                            args.next()
                                .ok_or_else(|| {
                                    anyhow::anyhow!("missing value for --leader-node-id")
                                })?
                                .parse()?,
                        )
                    }
                    "--voters" => {
                        voters =
                            Some(parse_node_id_list(&args.next().ok_or_else(|| {
                                anyhow::anyhow!("missing value for --voters")
                            })?)?)
                    }
                    "--learners" => {
                        learners = parse_node_id_list(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --learners"))?,
                        )?
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
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
                            kind: kind
                                .ok_or_else(|| anyhow::anyhow!("bootstrap requires --kind"))?,
                            tenant_id: tenant_id
                                .ok_or_else(|| anyhow::anyhow!("bootstrap requires --tenant-id"))?,
                            scope: scope
                                .ok_or_else(|| anyhow::anyhow!("bootstrap requires --scope"))?,
                        },
                        RangeBoundary::new(start_key, end_key),
                        epoch,
                        config_version,
                        leader,
                        voters
                            .iter()
                            .copied()
                            .map(|node_id| {
                                RangeReplica::new(
                                    node_id,
                                    ReplicaRole::Voter,
                                    ReplicaSyncState::Replicating,
                                )
                            })
                            .chain(learners.iter().copied().map(|node_id| {
                                RangeReplica::new(
                                    node_id,
                                    ReplicaRole::Learner,
                                    ReplicaSyncState::Replicating,
                                )
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
            let first = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range target"))?;
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut split_key = None;
            let mut output = output_mode();
            let mut current = Some(first);
            while let Some(flag) = current.take().or_else(|| args.next()) {
                match flag.as_str() {
                    value if !value.starts_with("--") && range_id.is_none() => {
                        range_id = Some(value.to_string());
                        split_key = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing split_key"))?
                                .into_bytes(),
                        );
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--split-key" => {
                        split_key = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --split-key"))?
                                .into_bytes(),
                        )
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range split flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::Split {
                    target: finish_range_target(range_id, kind, tenant_id, scope)?,
                    split_key: split_key.ok_or_else(|| {
                        anyhow::anyhow!("split requires split_key or --split-key")
                    })?,
                },
                output,
            ))
        }
        "merge" => {
            let left_range_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing left_range_id"))?;
            let right_range_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing right_range_id"))?;
            let mut output = output_mode();
            while let Some(flag) = args.next() {
                match flag.as_str() {
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range merge flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::Merge {
                    left_range_id,
                    right_range_id,
                },
                output,
            ))
        }
        "drain" | "retire" => {
            let first = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range target"))?;
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut output = output_mode();
            let mut current = Some(first);
            while let Some(flag) = current.take().or_else(|| args.next()) {
                match flag.as_str() {
                    value if !value.starts_with("--") && range_id.is_none() => {
                        range_id = Some(value.to_string());
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range {subcommand} flag: {flag}"),
                }
            }
            let target = finish_range_target(range_id, kind, tenant_id, scope)?;
            let command = if subcommand == "drain" {
                RangeCliCommand::Drain { target }
            } else {
                RangeCliCommand::Retire { target }
            };
            Ok((command, output))
        }
        "recover" => {
            let first = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range target"))?;
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut new_leader_node_id = None;
            let mut output = output_mode();
            let mut current = Some(first);
            while let Some(flag) = current.take().or_else(|| args.next()) {
                match flag.as_str() {
                    value if !value.starts_with("--") && range_id.is_none() => {
                        range_id = Some(value.to_string());
                        new_leader_node_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing new_leader_node_id"))?
                                .parse()?,
                        );
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--new-leader-node-id" => {
                        new_leader_node_id = Some(
                            args.next()
                                .ok_or_else(|| {
                                    anyhow::anyhow!("missing value for --new-leader-node-id")
                                })?
                                .parse()?,
                        )
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range recover flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::Recover {
                    target: finish_range_target(range_id, kind, tenant_id, scope)?,
                    new_leader_node_id: new_leader_node_id.ok_or_else(|| {
                        anyhow::anyhow!(
                            "recover requires new_leader_node_id or --new-leader-node-id"
                        )
                    })?,
                },
                output,
            ))
        }
        "change-replicas" => {
            let first = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range target"))?;
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut voters = None;
            let mut learners = Vec::new();
            let mut output = output_mode();
            let mut current = Some(first);
            while let Some(flag) = current.take().or_else(|| args.next()) {
                match flag.as_str() {
                    value if !value.starts_with("--") && range_id.is_none() => {
                        range_id = Some(value.to_string());
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--voters" => {
                        voters =
                            Some(parse_node_id_list(&args.next().ok_or_else(|| {
                                anyhow::anyhow!("missing value for --voters")
                            })?)?)
                    }
                    "--learners" => {
                        learners = parse_node_id_list(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --learners"))?,
                        )?
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range change-replicas flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::ChangeReplicas {
                    target: finish_range_target(range_id, kind, tenant_id, scope)?,
                    voters: voters
                        .ok_or_else(|| anyhow::anyhow!("change-replicas requires --voters"))?,
                    learners,
                },
                output,
            ))
        }
        "transfer-leadership" => {
            let first = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing range target"))?;
            let mut range_id = None;
            let mut kind = None;
            let mut tenant_id = None;
            let mut scope = None;
            let mut target_node_id = None;
            let mut output = output_mode();
            let mut current = Some(first);
            while let Some(flag) = current.take().or_else(|| args.next()) {
                match flag.as_str() {
                    value if !value.starts_with("--") && range_id.is_none() => {
                        range_id = Some(value.to_string());
                        target_node_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing target_node_id"))?
                                .parse()?,
                        );
                    }
                    "--kind" => {
                        kind =
                            Some(parse_service_shard_kind(&args.next().ok_or_else(
                                || anyhow::anyhow!("missing value for --kind"),
                            )?)?)
                    }
                    "--tenant-id" => {
                        tenant_id = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --tenant-id"))?,
                        )
                    }
                    "--scope" => {
                        scope = Some(
                            args.next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --scope"))?,
                        )
                    }
                    "--target-node-id" => {
                        target_node_id = Some(
                            args.next()
                                .ok_or_else(|| {
                                    anyhow::anyhow!("missing value for --target-node-id")
                                })?
                                .parse()?,
                        )
                    }
                    "--output" => {
                        output = parse_output_mode(
                            &args
                                .next()
                                .ok_or_else(|| anyhow::anyhow!("missing value for --output"))?,
                        )?
                    }
                    _ => anyhow::bail!("unknown range transfer-leadership flag: {flag}"),
                }
            }
            Ok((
                RangeCliCommand::TransferLeadership {
                    target: finish_range_target(range_id, kind, tenant_id, scope)?,
                    target_node_id: target_node_id.ok_or_else(|| {
                        anyhow::anyhow!(
                            "transfer-leadership requires target_node_id or --target-node-id"
                        )
                    })?,
                },
                output,
            ))
        }
        _ => anyhow::bail!("unknown range subcommand: {subcommand}"),
    }
}

pub(crate) fn shard_command_request(
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

pub(crate) fn parse_output_mode(value: &str) -> anyhow::Result<OutputMode> {
    match value {
        "json" => Ok(OutputMode::Json),
        "text" => Ok(OutputMode::Text),
        _ => anyhow::bail!("invalid output mode: {value}"),
    }
}

pub(crate) fn render_shard_response_text(body: &str) -> anyhow::Result<String> {
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

pub(crate) fn http_request_body(
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
