mod dist;
mod trie;

pub use dist::{
    dist_route_shard, dist_tenant_shard, DistBalanceAction, DistBalancePolicy, DistDeliveryReport,
    DistDeliverySink, DistFanoutRequest, DistFanoutWorker, DistHandle, DistMaintenance,
    DistMaintenanceWorker, DistRouter, DistTenantStats, PersistentDistHandle, ReplicatedDistHandle,
    ThresholdDistBalancePolicy,
};

pub trait DistRuntime: DistRouter + DistMaintenance {}

impl<T> DistRuntime for T where T: DistRouter + DistMaintenance + ?Sized {}

#[cfg(test)]
mod tests;
