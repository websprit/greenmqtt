use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListenerSpec {
    pub(crate) bind: SocketAddr,
    pub(crate) profile: String,
}

#[derive(Clone)]
pub(crate) struct PortMappedAuth {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredAuth>>,
}

#[derive(Clone)]
pub(crate) struct PortMappedAcl {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredAcl>>,
}

#[derive(Clone)]
pub(crate) struct PortMappedEventHook {
    default_profile: String,
    profiles: Arc<HashMap<String, ConfiguredEventHook>>,
}

impl PortMappedAuth {
    pub(crate) fn new(
        default_profile: impl Into<String>,
        profiles: HashMap<String, ConfiguredAuth>,
    ) -> Self {
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
    pub(crate) fn new(
        default_profile: impl Into<String>,
        profiles: HashMap<String, ConfiguredAcl>,
    ) -> Self {
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
    pub(crate) fn new(
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

pub(crate) fn configured_hooks_for_profile(
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

pub(crate) fn configured_hooks(node_id: u64) -> anyhow::Result<ConfiguredEventHook> {
    configured_hooks_for_profile(node_id, "default")
}

pub(crate) fn parse_topic_rewrite_rules(raw: &str) -> anyhow::Result<Vec<TopicRewriteRule>> {
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

pub(crate) fn parse_listener_specs(raw: &str) -> anyhow::Result<Vec<ListenerSpec>> {
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

pub(crate) fn listener_specs_from_env(
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

pub(crate) fn collect_listener_profiles(listener_groups: &[&[ListenerSpec]]) -> Vec<String> {
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

pub(crate) fn configured_listener_profiles(
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

pub(crate) fn default_listener_profiles(
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

pub(crate) fn configured_webhook(profile: &str) -> anyhow::Result<Option<WebHookEventHook>> {
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

pub(crate) fn configured_auth_for_profile(profile: &str) -> anyhow::Result<ConfiguredAuth> {
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
        return Ok(ConfiguredAuth::Http(HttpAuthProvider::new(HttpAuthConfig {
            url: http_auth_url.trim().to_string(),
        })));
    }
    Ok(ConfiguredAuth::default())
}

pub(crate) fn configured_auth() -> anyhow::Result<ConfiguredAuth> {
    configured_auth_for_profile("default")
}

pub(crate) fn configured_acl_for_profile(profile: &str) -> anyhow::Result<ConfiguredAcl> {
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

pub(crate) fn configured_acl() -> anyhow::Result<ConfiguredAcl> {
    configured_acl_for_profile("default")
}

pub(crate) fn parse_identity_matchers(raw: &str) -> anyhow::Result<Vec<IdentityMatcher>> {
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

pub(crate) fn parse_acl_rules(raw: &str) -> anyhow::Result<Vec<AclRule>> {
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

pub(crate) fn parse_bridge_rules(raw: &str) -> anyhow::Result<Vec<BridgeRule>> {
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
