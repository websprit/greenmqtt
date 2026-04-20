use super::*;

impl RpcRuntime {
    pub async fn serve(self, bind: SocketAddr) -> anyhow::Result<()> {
        Server::builder()
            .add_service(SessionDictServiceServer::new(SessionDictRpc {
                inner: self.sessiondict,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(DistServiceServer::new(DistRpc {
                inner: self.dist,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(InboxServiceServer::new(InboxRpc {
                inner: self.inbox,
                assignment_registry: self.assignment_registry.clone(),
            }))
            .add_service(RetainServiceServer::new(RetainRpc { inner: self.retain }))
            .add_service(BrokerPeerServiceServer::new(BrokerPeerRpc {
                inner: self.peer_sink,
            }))
            .add_service(MetadataServiceServer::new(MetadataRpc {
                inner: self.assignment_registry.clone(),
            }))
            .add_service(KvRangeServiceServer::new(KvRangeRpc {
                inner: self.range_host.clone(),
            }))
            .add_service(RaftTransportServiceServer::new(RaftTransportRpc {
                inner: self.range_host.clone(),
            }))
            .add_service(RangeAdminServiceServer::new(RangeAdminRpc {
                host: self.range_host.clone(),
                runtime: self.range_runtime.clone(),
            }))
            .add_service(RangeControlServiceServer::new(RangeControlRpc {
                inner: self.range_runtime.clone(),
                registry: self.assignment_registry.clone(),
            }))
            .serve(bind)
            .await?;
        Ok(())
    }
}

