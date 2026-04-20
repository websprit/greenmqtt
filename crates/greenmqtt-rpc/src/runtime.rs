use super::*;

impl RpcRuntime {
    pub async fn serve(self, bind: SocketAddr) -> anyhow::Result<()> {
        let governor = Arc::new(RpcTrafficGovernor::new(
            RpcTrafficGovernorConfig::from_env()?
        ));
        self.serve_with_governor(bind, governor).await
    }

    pub async fn serve_with_governor(
        self,
        bind: SocketAddr,
        governor: Arc<RpcTrafficGovernor>,
    ) -> anyhow::Result<()> {
        Server::builder()
            .add_service(RpcGovernedService::new(
                SessionDictServiceServer::new(SessionDictRpc {
                    inner: self.sessiondict,
                    assignment_registry: self.assignment_registry.clone(),
                }),
                governor.service(RpcServiceKind::SessionDict),
            ))
            .add_service(RpcGovernedService::new(
                DistServiceServer::new(DistRpc {
                    inner: self.dist,
                    assignment_registry: self.assignment_registry.clone(),
                }),
                governor.service(RpcServiceKind::Dist),
            ))
            .add_service(RpcGovernedService::new(
                InboxServiceServer::new(InboxRpc {
                    inner: self.inbox,
                    assignment_registry: self.assignment_registry.clone(),
                    lwt_sink: self.inbox_lwt_sink.clone(),
                }),
                governor.service(RpcServiceKind::Inbox),
            ))
            .add_service(RpcGovernedService::new(
                RetainServiceServer::new(RetainRpc { inner: self.retain }),
                governor.service(RpcServiceKind::Retain),
            ))
            .add_service(BrokerPeerServiceServer::new(BrokerPeerRpc {
                inner: self.peer_sink,
            }))
            .add_service(RpcGovernedService::new(
                MetadataServiceServer::new(MetadataRpc {
                    inner: self.assignment_registry.clone(),
                }),
                governor.service(RpcServiceKind::Metadata),
            ))
            .add_service(RpcGovernedService::new(
                KvRangeServiceServer::new(KvRangeRpc {
                    inner: self.range_host.clone(),
                }),
                governor.service(RpcServiceKind::KvRange),
            ))
            .add_service(RaftTransportServiceServer::new(RaftTransportRpc {
                inner: self.range_host.clone(),
            }))
            .add_service(RangeAdminServiceServer::new(RangeAdminRpc {
                host: self.range_host.clone(),
                runtime: self.range_runtime.clone(),
            }))
            .add_service(RpcGovernedService::new(
                RangeControlServiceServer::new(RangeControlRpc {
                    inner: self.range_runtime.clone(),
                    registry: self.assignment_registry.clone(),
                }),
                governor.service(RpcServiceKind::RangeControl),
            ))
            .serve(bind)
            .await?;
        Ok(())
    }
}
