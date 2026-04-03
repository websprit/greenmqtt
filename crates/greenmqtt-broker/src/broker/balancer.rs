use greenmqtt_core::ClientIdentity;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    endpoints: Vec<String>,
    next_index: AtomicUsize,
    permanent: bool,
}

impl RoundRobinBalancer {
    pub fn new(endpoints: Vec<String>, permanent: bool) -> Self {
        Self {
            endpoints,
            next_index: AtomicUsize::new(0),
            permanent,
        }
    }
}

impl ClientBalancer for RoundRobinBalancer {
    fn need_redirect(&self, _identity: &ClientIdentity) -> Option<Redirection> {
        if self.endpoints.is_empty() {
            return None;
        }
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        Some(Redirection {
            permanent: self.permanent,
            server_reference: self.endpoints[index % self.endpoints.len()].clone(),
        })
    }
}

pub(crate) type SharedClientBalancer = Arc<dyn ClientBalancer>;
