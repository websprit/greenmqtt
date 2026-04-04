use crate::PeerRegistry;
use greenmqtt_core::{ClientIdentity, NodeId};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

type SharedLoadGauge = Arc<dyn Fn() -> usize + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redirection {
    pub permanent: bool,
    pub server_reference: String,
}

pub trait ClientBalancer: Send + Sync {
    fn need_redirect(&self, identity: &ClientIdentity) -> Option<Redirection>;
}

#[derive(Default)]
pub(crate) struct NoopClientBalancer;

impl ClientBalancer for NoopClientBalancer {
    fn need_redirect(&self, _identity: &ClientIdentity) -> Option<Redirection> {
        None
    }
}

pub struct RoundRobinBalancer {
    local_node_id: NodeId,
    registry: Arc<dyn PeerRegistry>,
    local_load: SharedLoadGauge,
    redirect_threshold: usize,
    next_index: AtomicUsize,
    permanent: bool,
}

impl RoundRobinBalancer {
    pub fn new(local_node_id: NodeId, registry: Arc<dyn PeerRegistry>, permanent: bool) -> Self {
        Self::with_threshold(
            local_node_id,
            registry,
            Arc::new(|| usize::MAX),
            0,
            permanent,
        )
    }

    pub fn with_threshold(
        local_node_id: NodeId,
        registry: Arc<dyn PeerRegistry>,
        local_load: SharedLoadGauge,
        redirect_threshold: usize,
        permanent: bool,
    ) -> Self {
        Self {
            local_node_id,
            registry,
            local_load,
            redirect_threshold,
            next_index: AtomicUsize::new(0),
            permanent,
        }
    }

    fn candidate_endpoints(&self) -> Vec<String> {
        let endpoints: BTreeMap<_, _> = self.registry.list_peer_endpoints();
        endpoints
            .into_iter()
            .filter(|(node_id, _)| *node_id != self.local_node_id)
            .map(|(_, endpoint)| endpoint)
            .collect()
    }
}

impl ClientBalancer for RoundRobinBalancer {
    fn need_redirect(&self, _identity: &ClientIdentity) -> Option<Redirection> {
        if (self.local_load)() < self.redirect_threshold {
            return None;
        }
        let endpoints = self.candidate_endpoints();
        if endpoints.is_empty() {
            return None;
        }
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        Some(Redirection {
            permanent: self.permanent,
            server_reference: endpoints[index % endpoints.len()].clone(),
        })
    }
}

pub(crate) type SharedClientBalancer = Arc<dyn ClientBalancer>;
