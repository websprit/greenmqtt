mod memory;
mod redis;
mod rocksdb;
mod sled;
mod traits;

pub use memory::{
    MemoryInboxStore, MemoryInflightStore, MemoryRetainStore, MemoryRouteStore, MemorySessionStore,
    MemorySubscriptionStore,
};
pub use redis::{
    RedisInboxStore, RedisInflightStore, RedisRetainStore, RedisRouteStore, RedisSessionStore,
    RedisSubscriptionStore,
};
pub use rocksdb::{
    RocksInboxStore, RocksInflightStore, RocksRetainStore, RocksRouteStore, RocksSessionStore,
    RocksSubscriptionStore,
};
pub use sled::{
    SledInboxStore, SledInflightStore, SledRetainStore, SledRouteStore, SledSessionStore,
    SledSubscriptionStore,
};
pub use traits::{
    InboxStore, InflightStore, RetainStore, RouteStore, SessionStore, SubscriptionStore,
};

use ::rocksdb::{Options, DB};
use ::sled::Tree;
use greenmqtt_core::{RouteRecord, Subscription};
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn session_key(tenant_id: &str, client_id: &str) -> Vec<u8> {
    composite_key(&[tenant_id.as_bytes(), client_id.as_bytes()])
}

fn session_prefix(tenant_id: &str) -> Vec<u8> {
    prefixed_key(&[tenant_id.as_bytes()])
}

const SESSION_ID_INDEX_PREFIX: &[u8] = &[0xFF, b's', b'i', b'd', 0];
const ROUTE_SESSION_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b's', b'i', 0];
const ROUTE_SESSION_TOPIC_SHARED_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b's', b't', 0];
const ROUTE_TENANT_SHARED_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b't', b's', 0];
const ROUTE_FILTER_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b'f', b'i', 0];
const ROUTE_FILTER_SHARED_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b'f', b's', 0];
const ROUTE_EXACT_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b'e', b'i', 0];
const ROUTE_WILDCARD_INDEX_PREFIX: &[u8] = &[0xFF, b'r', b'w', b'i', 0];
const INBOX_TENANT_INDEX_PREFIX: &[u8] = &[0xFF, b'i', b't', b'i', 0];
const INFLIGHT_TENANT_INDEX_PREFIX: &[u8] = &[0xFF, b'i', b'f', b't', 0];
const SESSION_COUNT_KEY: &[u8] = &[0xFF, b's', b'c', 0];
const SUBSCRIPTION_COUNT_KEY: &[u8] = &[0xFF, b's', b'u', b'c', 0];
const INBOX_COUNT_KEY: &[u8] = &[0xFF, b'i', b'c', 0];
const INFLIGHT_COUNT_KEY: &[u8] = &[0xFF, b'i', b'f', b'c', 0];
const RETAIN_COUNT_KEY: &[u8] = &[0xFF, b'r', b'c', 0];
const ROUTE_COUNT_KEY: &[u8] = &[0xFF, b'r', b't', b'c', 0];

fn session_id_index_key(session_id: &str) -> Vec<u8> {
    let mut key = SESSION_ID_INDEX_PREFIX.to_vec();
    key.extend_from_slice(session_id.as_bytes());
    key
}

fn is_session_id_index_key(key: &[u8]) -> bool {
    key.starts_with(SESSION_ID_INDEX_PREFIX)
}

fn is_session_internal_key(key: &[u8]) -> bool {
    is_session_id_index_key(key) || key == SESSION_COUNT_KEY
}

fn route_session_index_key(route: &RouteRecord) -> Vec<u8> {
    let mut key = ROUTE_SESSION_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        route.session_id.as_bytes(),
        route.tenant_id.as_bytes(),
        route.shared_group.as_deref().unwrap_or_default().as_bytes(),
        route.topic_filter.as_bytes(),
    ]));
    key
}

fn route_session_index_prefix(session_id: &str) -> Vec<u8> {
    let mut key = ROUTE_SESSION_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[session_id.as_bytes()]));
    key
}

fn is_route_session_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_SESSION_INDEX_PREFIX)
}

fn route_session_topic_shared_index_key(route: &RouteRecord) -> Vec<u8> {
    route_session_topic_shared_index_key_from_parts(
        &route.session_id,
        &route.topic_filter,
        route.shared_group.as_deref(),
    )
}

fn route_session_topic_shared_index_key_from_parts(
    session_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> Vec<u8> {
    let mut key = ROUTE_SESSION_TOPIC_SHARED_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        session_id.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
        topic_filter.as_bytes(),
    ]));
    key
}

fn is_route_session_topic_shared_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_SESSION_TOPIC_SHARED_INDEX_PREFIX)
}

fn route_tenant_shared_index_key(route: &RouteRecord) -> Vec<u8> {
    route_tenant_shared_index_key_from_parts(
        &route.tenant_id,
        route.shared_group.as_deref(),
        &route.session_id,
        &route.topic_filter,
    )
}

fn route_tenant_shared_index_key_from_parts(
    tenant_id: &str,
    shared_group: Option<&str>,
    session_id: &str,
    topic_filter: &str,
) -> Vec<u8> {
    let mut key = ROUTE_TENANT_SHARED_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        tenant_id.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
        session_id.as_bytes(),
        topic_filter.as_bytes(),
    ]));
    key
}

fn route_tenant_shared_index_prefix(tenant_id: &str, shared_group: Option<&str>) -> Vec<u8> {
    let mut key = ROUTE_TENANT_SHARED_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[
        tenant_id.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
    ]));
    key
}

fn is_route_tenant_shared_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_TENANT_SHARED_INDEX_PREFIX)
}

fn route_filter_index_key(route: &RouteRecord) -> Vec<u8> {
    let mut key = ROUTE_FILTER_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        route.tenant_id.as_bytes(),
        route.topic_filter.as_bytes(),
        route.session_id.as_bytes(),
        route.shared_group.as_deref().unwrap_or_default().as_bytes(),
    ]));
    key
}

fn route_filter_index_prefix(tenant_id: &str, topic_filter: &str) -> Vec<u8> {
    let mut key = ROUTE_FILTER_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
    ]));
    key
}

fn is_route_filter_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_FILTER_INDEX_PREFIX)
}

fn route_filter_shared_index_key(route: &RouteRecord) -> Vec<u8> {
    route_filter_shared_index_key_from_parts(
        &route.tenant_id,
        &route.topic_filter,
        route.shared_group.as_deref(),
        &route.session_id,
    )
}

fn route_filter_shared_index_key_from_parts(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
    session_id: &str,
) -> Vec<u8> {
    let mut key = ROUTE_FILTER_SHARED_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
        session_id.as_bytes(),
    ]));
    key
}

fn route_filter_shared_index_prefix(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> Vec<u8> {
    let mut key = ROUTE_FILTER_SHARED_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
    ]));
    key
}

fn is_route_filter_shared_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_FILTER_SHARED_INDEX_PREFIX)
}

fn route_exact_index_key(route: &RouteRecord) -> Vec<u8> {
    let mut key = ROUTE_EXACT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        route.tenant_id.as_bytes(),
        route.topic_filter.as_bytes(),
        route.session_id.as_bytes(),
        route.shared_group.as_deref().unwrap_or_default().as_bytes(),
    ]));
    key
}

fn route_exact_index_prefix(tenant_id: &str, topic_filter: &str) -> Vec<u8> {
    let mut key = ROUTE_EXACT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
    ]));
    key
}

fn is_route_exact_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_EXACT_INDEX_PREFIX)
}

fn route_wildcard_index_key(route: &RouteRecord) -> Vec<u8> {
    let mut key = ROUTE_WILDCARD_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        route.tenant_id.as_bytes(),
        route.session_id.as_bytes(),
        route.shared_group.as_deref().unwrap_or_default().as_bytes(),
        route.topic_filter.as_bytes(),
    ]));
    key
}

fn route_wildcard_index_prefix(tenant_id: &str) -> Vec<u8> {
    let mut key = ROUTE_WILDCARD_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[tenant_id.as_bytes()]));
    key
}

fn is_route_wildcard_index_key(key: &[u8]) -> bool {
    key.starts_with(ROUTE_WILDCARD_INDEX_PREFIX)
}

fn inbox_tenant_index_key(tenant_id: &str, session_id: &str, seq: u64) -> Vec<u8> {
    let mut key = INBOX_TENANT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        tenant_id.as_bytes(),
        session_id.as_bytes(),
    ]));
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn inbox_tenant_prefix(tenant_id: &str) -> Vec<u8> {
    let mut key = INBOX_TENANT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[tenant_id.as_bytes()]));
    key
}

fn is_inbox_tenant_index_key(key: &[u8]) -> bool {
    key.starts_with(INBOX_TENANT_INDEX_PREFIX)
}

fn is_inbox_internal_key(key: &[u8]) -> bool {
    is_inbox_tenant_index_key(key) || key == INBOX_COUNT_KEY
}

fn inflight_tenant_index_key(tenant_id: &str, session_id: &str, packet_id: u16) -> Vec<u8> {
    let mut key = INFLIGHT_TENANT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&composite_key(&[
        tenant_id.as_bytes(),
        session_id.as_bytes(),
    ]));
    key.extend_from_slice(&packet_id.to_be_bytes());
    key
}

fn inflight_tenant_prefix(tenant_id: &str) -> Vec<u8> {
    let mut key = INFLIGHT_TENANT_INDEX_PREFIX.to_vec();
    key.extend_from_slice(&prefixed_key(&[tenant_id.as_bytes()]));
    key
}

fn is_inflight_tenant_index_key(key: &[u8]) -> bool {
    key.starts_with(INFLIGHT_TENANT_INDEX_PREFIX)
}

fn is_inflight_internal_key(key: &[u8]) -> bool {
    is_inflight_tenant_index_key(key) || key == INFLIGHT_COUNT_KEY
}

fn is_route_internal_key(key: &[u8]) -> bool {
    is_route_session_index_key(key)
        || is_route_session_topic_shared_index_key(key)
        || is_route_tenant_shared_index_key(key)
        || is_route_filter_index_key(key)
        || is_route_filter_shared_index_key(key)
        || is_route_exact_index_key(key)
        || is_route_wildcard_index_key(key)
        || key == ROUTE_COUNT_KEY
}

fn subscription_key(session_id: &str, shared_group: Option<&str>, topic_filter: &str) -> Vec<u8> {
    composite_key(&[
        session_id.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
        topic_filter.as_bytes(),
    ])
}

pub(crate) fn subscription_identity(
    session_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        session_id.to_string(),
        shared_group.unwrap_or_default().to_string(),
        topic_filter.to_string(),
    )
}

pub(crate) fn subscription_tenant_topic_shared_identity(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        tenant_id.to_string(),
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

fn subscription_prefix(session_id: &str) -> Vec<u8> {
    prefixed_key(&[session_id.as_bytes()])
}

fn subscription_tenant_index_key(subscription: &Subscription) -> Vec<u8> {
    composite_key(&[
        b"tenant-subscription",
        subscription.tenant_id.as_bytes(),
        subscription.session_id.as_bytes(),
        subscription
            .shared_group
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        subscription.topic_filter.as_bytes(),
    ])
}

fn subscription_tenant_shared_index_key(subscription: &Subscription) -> Vec<u8> {
    composite_key(&[
        b"tenant-shared-subscription",
        subscription.tenant_id.as_bytes(),
        subscription
            .shared_group
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        subscription.session_id.as_bytes(),
        subscription.topic_filter.as_bytes(),
    ])
}

fn subscription_tenant_topic_index_key(subscription: &Subscription) -> Vec<u8> {
    composite_key(&[
        b"tenant-topic-subscription",
        subscription.tenant_id.as_bytes(),
        subscription.topic_filter.as_bytes(),
        subscription.session_id.as_bytes(),
        subscription
            .shared_group
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    ])
}

fn subscription_tenant_topic_shared_index_key(subscription: &Subscription) -> Vec<u8> {
    composite_key(&[
        b"tenant-topic-shared-subscription",
        subscription.tenant_id.as_bytes(),
        subscription.topic_filter.as_bytes(),
        subscription
            .shared_group
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
        subscription.session_id.as_bytes(),
    ])
}

fn subscription_tenant_prefix(tenant_id: &str) -> Vec<u8> {
    composite_key(&[b"tenant-subscription", tenant_id.as_bytes()])
}

fn subscription_tenant_shared_prefix(tenant_id: &str, shared_group: Option<&str>) -> Vec<u8> {
    composite_key(&[
        b"tenant-shared-subscription",
        tenant_id.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
    ])
}

fn subscription_tenant_topic_prefix(tenant_id: &str, topic_filter: &str) -> Vec<u8> {
    composite_key(&[
        b"tenant-topic-subscription",
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
    ])
}

fn subscription_tenant_topic_shared_prefix(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> Vec<u8> {
    composite_key(&[
        b"tenant-topic-shared-subscription",
        tenant_id.as_bytes(),
        topic_filter.as_bytes(),
        shared_group.unwrap_or_default().as_bytes(),
    ])
}

fn is_subscription_tenant_index_key(key: &[u8]) -> bool {
    key.starts_with(b"tenant-subscription\0")
}

fn is_subscription_tenant_shared_index_key(key: &[u8]) -> bool {
    key.starts_with(b"tenant-shared-subscription\0")
}

fn is_subscription_tenant_topic_index_key(key: &[u8]) -> bool {
    key.starts_with(b"tenant-topic-subscription\0")
}

fn is_subscription_tenant_topic_shared_index_key(key: &[u8]) -> bool {
    key.starts_with(b"tenant-topic-shared-subscription\0")
}

fn is_subscription_internal_key(key: &[u8]) -> bool {
    is_subscription_tenant_index_key(key)
        || is_subscription_tenant_shared_index_key(key)
        || is_subscription_tenant_topic_index_key(key)
        || is_subscription_tenant_topic_shared_index_key(key)
        || key == SUBSCRIPTION_COUNT_KEY
}

fn inbox_key(session_id: &str, seq: u64) -> Vec<u8> {
    let mut key = prefixed_key(&[session_id.as_bytes()]);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn inbox_prefix(session_id: &str) -> Vec<u8> {
    prefixed_key(&[session_id.as_bytes()])
}

fn inflight_key(session_id: &str, packet_id: u16) -> Vec<u8> {
    let mut key = prefixed_key(&[session_id.as_bytes()]);
    key.extend_from_slice(&packet_id.to_be_bytes());
    key
}

fn inflight_prefix(session_id: &str) -> Vec<u8> {
    prefixed_key(&[session_id.as_bytes()])
}

fn trailing_u64(key: &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = key[key.len() - 8..].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn encode_count(count: usize) -> [u8; 8] {
    (count as u64).to_be_bytes()
}

fn decode_count(bytes: &[u8]) -> anyhow::Result<usize> {
    let count: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid count bytes"))?;
    Ok(u64::from_be_bytes(count) as usize)
}

fn apply_count_delta(current: usize, delta: isize) -> anyhow::Result<usize> {
    if delta >= 0 {
        Ok(current + delta as usize)
    } else {
        current
            .checked_sub((-delta) as usize)
            .ok_or_else(|| anyhow::anyhow!("count underflow"))
    }
}

fn read_sled_count(tree: &Tree, key: &[u8]) -> anyhow::Result<usize> {
    tree.get(key)?
        .map(|value| decode_count(value.as_ref()))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

fn update_sled_count(tree: &Tree, key: &[u8], delta: isize) -> anyhow::Result<()> {
    let next = apply_count_delta(read_sled_count(tree, key)?, delta)?;
    tree.insert(key, &encode_count(next))?;
    Ok(())
}

fn read_rocks_count(db: &DB, key: &[u8]) -> anyhow::Result<usize> {
    match db.get(key)? {
        Some(value) => decode_count(value.as_ref()),
        None => Ok(0),
    }
}

fn retain_key(tenant_id: &str, topic: &str) -> Vec<u8> {
    composite_key(&[tenant_id.as_bytes(), topic.as_bytes()])
}

fn retain_prefix(tenant_id: &str) -> Vec<u8> {
    prefixed_key(&[tenant_id.as_bytes()])
}

fn route_key(route: &RouteRecord) -> Vec<u8> {
    composite_key(&[
        route.tenant_id.as_bytes(),
        route.session_id.as_bytes(),
        route.shared_group.as_deref().unwrap_or_default().as_bytes(),
        route.topic_filter.as_bytes(),
    ])
}

fn route_prefix(tenant_id: &str) -> Vec<u8> {
    prefixed_key(&[tenant_id.as_bytes()])
}

pub(crate) fn route_session_topic_shared_identity(
    session_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        session_id.to_string(),
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

pub(crate) fn route_tenant_shared_identity(
    tenant_id: &str,
    shared_group: Option<&str>,
) -> (String, String) {
    (
        tenant_id.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

pub(crate) fn route_filter_shared_identity(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> (String, String, String) {
    (
        tenant_id.to_string(),
        topic_filter.to_string(),
        shared_group.unwrap_or_default().to_string(),
    )
}

pub(crate) fn route_topic_filter_is_exact(topic_filter: &str) -> bool {
    !topic_filter.contains('#') && !topic_filter.contains('+')
}

fn composite_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(part);
        key.push(0);
    }
    key
}

fn prefixed_key(parts: &[&[u8]]) -> Vec<u8> {
    composite_key(parts)
}

const ROCKS_VALUE_ENCODING_VERSION: u8 = 1;
const ROCKS_BLOCK_CACHE_BYTES: usize = 32 * 1024 * 1024;
const ROCKS_MEMTABLE_BUDGET_BYTES: usize = 128 * 1024 * 1024;
const ROCKS_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const ROCKS_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;
const ROCKS_MAX_BACKGROUND_JOBS: i32 = 4;
const ROCKS_BYTES_PER_SYNC: u64 = 1 << 20;
const ROCKS_MEMTABLE_PREFIX_BLOOM_RATIO: f64 = 0.125;

#[derive(Debug, Clone, PartialEq)]
struct RocksDbConfig {
    block_cache_bytes: usize,
    memtable_budget_bytes: usize,
    write_buffer_bytes: usize,
    bloom_filter_bits_per_key: f64,
    max_background_jobs: i32,
    parallelism: i32,
    bytes_per_sync: u64,
    optimize_filters_for_hits: bool,
    memtable_whole_key_filtering: bool,
    memtable_prefix_bloom_ratio: f64,
}

impl Default for RocksDbConfig {
    fn default() -> Self {
        Self {
            block_cache_bytes: ROCKS_BLOCK_CACHE_BYTES,
            memtable_budget_bytes: ROCKS_MEMTABLE_BUDGET_BYTES,
            write_buffer_bytes: ROCKS_WRITE_BUFFER_BYTES,
            bloom_filter_bits_per_key: ROCKS_BLOOM_FILTER_BITS_PER_KEY,
            max_background_jobs: ROCKS_MAX_BACKGROUND_JOBS,
            parallelism: std::thread::available_parallelism()
                .map(|parallelism| parallelism.get())
                .unwrap_or(4)
                .min(i32::MAX as usize) as i32,
            bytes_per_sync: ROCKS_BYTES_PER_SYNC,
            optimize_filters_for_hits: true,
            memtable_whole_key_filtering: true,
            memtable_prefix_bloom_ratio: ROCKS_MEMTABLE_PREFIX_BLOOM_RATIO,
        }
    }
}

impl RocksDbConfig {
    fn from_env() -> anyhow::Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            block_cache_bytes: env_or_parse(
                "GREENMQTT_ROCKSDB_BLOCK_CACHE_BYTES",
                defaults.block_cache_bytes,
            )?,
            memtable_budget_bytes: env_or_parse(
                "GREENMQTT_ROCKSDB_MEMTABLE_BUDGET_BYTES",
                defaults.memtable_budget_bytes,
            )?,
            write_buffer_bytes: env_or_parse(
                "GREENMQTT_ROCKSDB_WRITE_BUFFER_BYTES",
                defaults.write_buffer_bytes,
            )?,
            bloom_filter_bits_per_key: env_or_parse(
                "GREENMQTT_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY",
                defaults.bloom_filter_bits_per_key,
            )?,
            max_background_jobs: env_or_parse(
                "GREENMQTT_ROCKSDB_MAX_BACKGROUND_JOBS",
                defaults.max_background_jobs,
            )?,
            parallelism: env_or_parse("GREENMQTT_ROCKSDB_PARALLELISM", defaults.parallelism)?,
            bytes_per_sync: env_or_parse(
                "GREENMQTT_ROCKSDB_BYTES_PER_SYNC",
                defaults.bytes_per_sync,
            )?,
            optimize_filters_for_hits: env_or_parse(
                "GREENMQTT_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS",
                defaults.optimize_filters_for_hits,
            )?,
            memtable_whole_key_filtering: env_or_parse(
                "GREENMQTT_ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING",
                defaults.memtable_whole_key_filtering,
            )?,
            memtable_prefix_bloom_ratio: env_or_parse(
                "GREENMQTT_ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO",
                defaults.memtable_prefix_bloom_ratio,
            )?,
        })
    }
}

fn env_or_parse<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("invalid {name} value `{value}`: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow::anyhow!("failed to read {name}: {error}")),
    }
}

fn encode_rocks_value<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(64);
    encoded.push(ROCKS_VALUE_ENCODING_VERSION);
    encoded.extend(bincode::serialize(value)?);
    Ok(encoded)
}

fn decode_rocks_value<T: DeserializeOwned>(value: &[u8]) -> anyhow::Result<T> {
    let (version, payload) = value
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("missing rocksdb value encoding version"))?;
    anyhow::ensure!(
        *version == ROCKS_VALUE_ENCODING_VERSION,
        "unsupported rocksdb value encoding version: {version}"
    );
    Ok(bincode::deserialize(payload)?)
}

fn rocks_prefix_extractor(key: &[u8]) -> &[u8] {
    if key.starts_with(&[0xFF]) {
        let mut zero_count = 0usize;
        for (index, byte) in key.iter().enumerate() {
            if *byte == 0 {
                zero_count += 1;
                if zero_count == 2 {
                    return &key[..=index];
                }
            }
        }
        return key;
    }

    for (index, byte) in key.iter().enumerate() {
        if *byte == 0 {
            return &key[..=index];
        }
    }
    key
}

fn open_rocks_db(path: impl AsRef<Path>) -> anyhow::Result<DB> {
    use ::rocksdb::{BlockBasedOptions, Cache, DBCompressionType, SliceTransform};

    let config = RocksDbConfig::from_env()?;
    let mut block_based = BlockBasedOptions::default();
    let block_cache = Cache::new_lru_cache(config.block_cache_bytes);
    block_based.set_block_cache(&block_cache);
    block_based.set_bloom_filter(config.bloom_filter_bits_per_key, true);

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_block_based_table_factory(&block_based);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_write_buffer_size(config.write_buffer_bytes);
    options.optimize_level_style_compaction(config.memtable_budget_bytes);
    options.set_max_background_jobs(config.max_background_jobs);
    options.increase_parallelism(config.parallelism);
    options.set_bytes_per_sync(config.bytes_per_sync);
    options.set_optimize_filters_for_hits(config.optimize_filters_for_hits);
    options.set_memtable_whole_key_filtering(config.memtable_whole_key_filtering);
    options.set_memtable_prefix_bloom_ratio(config.memtable_prefix_bloom_ratio);
    options.set_prefix_extractor(SliceTransform::create(
        "greenmqtt-prefix",
        rocks_prefix_extractor,
        None,
    ));
    Ok(DB::open(&options, path)?)
}

fn next_inbox_seq(seq: &AtomicU64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock drift before unix epoch")
        .as_nanos() as u64;
    let suffix = seq.fetch_add(1, Ordering::Relaxed) & 0xffff;
    (now << 16) | suffix
}

fn redis_prefix_pattern(kind: &str) -> String {
    format!("greenmqtt:{kind}:*")
}

fn redis_session_key(tenant_id: &str, client_id: &str) -> String {
    format!("greenmqtt:session:{tenant_id}:{client_id}")
}

fn redis_session_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:session:{tenant_id}:*")
}

fn redis_session_id_key(session_id: &str) -> String {
    format!("greenmqtt:session-id:{session_id}")
}

fn redis_session_count_key() -> &'static str {
    "greenmqtt:session-count"
}

fn redis_subscription_key(
    session_id: &str,
    shared_group: Option<&str>,
    topic_filter: &str,
) -> String {
    format!(
        "greenmqtt:subscription:{session_id}:{}:{topic_filter}",
        shared_group.unwrap_or_default()
    )
}

fn redis_subscription_pattern(session_id: &str) -> String {
    format!("greenmqtt:subscription:{session_id}:*")
}

fn redis_subscription_tenant_key(subscription: &Subscription) -> String {
    format!(
        "greenmqtt:subscription-tenant:{}:{}:{}:{}",
        subscription.tenant_id,
        subscription.session_id,
        subscription.shared_group.as_deref().unwrap_or_default(),
        subscription.topic_filter
    )
}

fn redis_subscription_tenant_shared_key(subscription: &Subscription) -> String {
    format!(
        "greenmqtt:subscription-tenant-shared:{}:{}:{}:{}",
        subscription.tenant_id,
        subscription.shared_group.as_deref().unwrap_or_default(),
        subscription.session_id,
        subscription.topic_filter
    )
}

fn redis_subscription_tenant_topic_key(subscription: &Subscription) -> String {
    format!(
        "greenmqtt:subscription-tenant-topic:{}:{}:{}:{}",
        subscription.tenant_id,
        subscription.topic_filter,
        subscription.session_id,
        subscription.shared_group.as_deref().unwrap_or_default()
    )
}

fn redis_subscription_tenant_topic_shared_key(subscription: &Subscription) -> String {
    format!(
        "greenmqtt:subscription-tenant-topic-shared:{}:{}:{}:{}",
        subscription.tenant_id,
        subscription.topic_filter,
        subscription.shared_group.as_deref().unwrap_or_default(),
        subscription.session_id,
    )
}

fn redis_subscription_tenant_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:subscription-tenant:{tenant_id}:*")
}

fn redis_subscription_tenant_shared_pattern(tenant_id: &str, shared_group: Option<&str>) -> String {
    format!(
        "greenmqtt:subscription-tenant-shared:{tenant_id}:{}:*",
        shared_group.unwrap_or_default()
    )
}

fn redis_subscription_tenant_topic_pattern(tenant_id: &str, topic_filter: &str) -> String {
    format!("greenmqtt:subscription-tenant-topic:{tenant_id}:{topic_filter}:*")
}

fn redis_subscription_tenant_topic_shared_pattern(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> String {
    format!(
        "greenmqtt:subscription-tenant-topic-shared:{tenant_id}:{topic_filter}:{}:*",
        shared_group.unwrap_or_default()
    )
}

fn redis_subscription_count_key() -> &'static str {
    "greenmqtt:subscription-count"
}

fn redis_inbox_key(session_id: &str) -> String {
    format!("greenmqtt:inbox:{session_id}")
}

fn redis_inbox_tenant_key(tenant_id: &str) -> String {
    format!("greenmqtt:inbox-tenant:{tenant_id}")
}

fn redis_inbox_count_key() -> &'static str {
    "greenmqtt:inbox-count"
}

fn redis_inflight_key(session_id: &str, packet_id: u16) -> String {
    format!("greenmqtt:inflight:{session_id}:{packet_id}")
}

fn redis_inflight_pattern(session_id: &str) -> String {
    format!("greenmqtt:inflight:{session_id}:*")
}

fn redis_inflight_tenant_key(tenant_id: &str, session_id: &str, packet_id: u16) -> String {
    format!("greenmqtt:inflight-tenant:{tenant_id}:{session_id}:{packet_id}")
}

fn redis_inflight_tenant_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:inflight-tenant:{tenant_id}:*")
}

fn redis_inflight_count_key() -> &'static str {
    "greenmqtt:inflight-count"
}

fn redis_retain_count_key() -> &'static str {
    "greenmqtt:retain-count"
}

fn redis_retain_key(tenant_id: &str, topic: &str) -> String {
    format!("greenmqtt:retain:{tenant_id}:{topic}")
}

fn redis_retain_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:retain:{tenant_id}:*")
}

fn redis_route_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route:{}:{}:{}:{}",
        route.tenant_id,
        route.session_id,
        route.shared_group.as_deref().unwrap_or_default(),
        route.topic_filter
    )
}

fn redis_route_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:route:{tenant_id}:*")
}

fn redis_route_session_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-session:{}:{}:{}:{}",
        route.session_id,
        route.tenant_id,
        route.shared_group.as_deref().unwrap_or_default(),
        route.topic_filter
    )
}

fn redis_route_session_pattern(session_id: &str) -> String {
    format!("greenmqtt:route-session:{session_id}:*")
}

fn redis_route_session_topic_shared_key(route: &RouteRecord) -> String {
    redis_route_session_topic_shared_key_from_parts(
        &route.session_id,
        &route.topic_filter,
        route.shared_group.as_deref(),
    )
}

fn redis_route_session_topic_shared_key_from_parts(
    session_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> String {
    format!(
        "greenmqtt:route-session-topic:{}:{}:{}",
        session_id,
        shared_group.unwrap_or_default(),
        topic_filter
    )
}

fn redis_route_tenant_shared_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-tenant-shared:{}:{}:{}:{}",
        route.tenant_id,
        route.shared_group.as_deref().unwrap_or_default(),
        route.session_id,
        route.topic_filter
    )
}

fn redis_route_tenant_shared_pattern(tenant_id: &str, shared_group: Option<&str>) -> String {
    format!(
        "greenmqtt:route-tenant-shared:{tenant_id}:{}:*",
        shared_group.unwrap_or_default()
    )
}

fn redis_route_filter_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-filter:{}:{}:{}:{}",
        route.tenant_id,
        route.topic_filter,
        route.session_id,
        route.shared_group.as_deref().unwrap_or_default()
    )
}

fn redis_route_filter_pattern(tenant_id: &str, topic_filter: &str) -> String {
    format!("greenmqtt:route-filter:{tenant_id}:{topic_filter}:*")
}

fn redis_route_filter_shared_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-filter-shared:{}:{}:{}:{}",
        route.tenant_id,
        route.topic_filter,
        route.shared_group.as_deref().unwrap_or_default(),
        route.session_id
    )
}

fn redis_route_filter_shared_pattern(
    tenant_id: &str,
    topic_filter: &str,
    shared_group: Option<&str>,
) -> String {
    format!(
        "greenmqtt:route-filter-shared:{tenant_id}:{topic_filter}:{}:*",
        shared_group.unwrap_or_default()
    )
}

fn redis_route_exact_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-exact:{}:{}:{}:{}",
        route.tenant_id,
        route.topic_filter,
        route.session_id,
        route.shared_group.as_deref().unwrap_or_default()
    )
}

fn redis_route_exact_pattern(tenant_id: &str, topic_filter: &str) -> String {
    format!("greenmqtt:route-exact:{tenant_id}:{topic_filter}:*")
}

fn redis_route_wildcard_key(route: &RouteRecord) -> String {
    format!(
        "greenmqtt:route-wildcard:{}:{}:{}:{}",
        route.tenant_id,
        route.session_id,
        route.shared_group.as_deref().unwrap_or_default(),
        route.topic_filter
    )
}

fn redis_route_wildcard_pattern(tenant_id: &str) -> String {
    format!("greenmqtt:route-wildcard:{tenant_id}:*")
}

fn redis_route_count_key() -> &'static str {
    "greenmqtt:route-count"
}

#[cfg(test)]
mod tests;
