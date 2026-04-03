use crate::broker::metrics::BrokerMetrics;
use crate::broker::send_quota::SendQuotaManager;
use crate::mqtt::connect::{
    build_connack_error_packet, build_connack_packet_with_properties, connect_error_reason_code,
    connect_error_reason_string, prepare_connect,
};
use crate::mqtt::state::ProtocolStateError;
use crate::mqtt::{
    build_pubrel_packet, delivery_properties_with_subscription_identifier,
    protocol_error_disconnect_reason_string,
};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

use super::codec::{parse_packet_frame, read_packet, Packet};
use super::util::packet_exceeds_limit;

#[async_trait]
pub(crate) trait SessionTransport: Send + Sync + Unpin + 'static {
    async fn read_packet(
        &mut self,
        protocol_level: u8,
        max_packet_size: Option<u32>,
    ) -> anyhow::Result<Option<Packet>>;
    async fn write_bytes(&mut self, payload: &[u8]) -> anyhow::Result<()>;
}

pub(crate) struct TcpTransport<S> {
    pub stream: S,
}

impl<S> TcpTransport<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl<S> SessionTransport for TcpTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn read_packet(
        &mut self,
        protocol_level: u8,
        max_packet_size: Option<u32>,
    ) -> anyhow::Result<Option<Packet>> {
        match read_packet(&mut self.stream, protocol_level, max_packet_size).await {
            Ok(packet) => Ok(Some(packet)),
            Err(e) => {
                if let Some(err) = e.downcast_ref::<std::io::Error>() {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                }
                Err(e)
            }
        }
    }

    async fn write_bytes(&mut self, payload: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(payload).await?;
        Ok(())
    }
}

pub(crate) struct WsTransport<S> {
    pub stream: WebSocketStream<S>,
}

impl<S> WsTransport<S> {
    pub(crate) fn new(stream: WebSocketStream<S>) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl<S> SessionTransport for WsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn read_packet(
        &mut self,
        protocol_level: u8,
        max_packet_size: Option<u32>,
    ) -> anyhow::Result<Option<Packet>> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Binary(frame))) => {
                    if packet_exceeds_limit(frame.len(), max_packet_size) {
                        anyhow::bail!("packet exceeds configured maximum packet size");
                    }
                    return Ok(Some(parse_packet_frame(&frame, protocol_level)?));
                }
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(Message::Ping(payload))) => {
                    self.stream.send(Message::Pong(payload)).await?;
                }
                Some(Ok(_)) => {} // Ignore Text/Pong frames
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(None),
            }
        }
    }

    async fn write_bytes(&mut self, payload: &[u8]) -> anyhow::Result<()> {
        self.stream.send(Message::Binary(payload.to_vec())).await?;
        Ok(())
    }
}

use crate::mqtt::auth::drive_session_reauth;
use crate::mqtt::auth::{drive_connect_enhanced_auth, ConnectAuthOutcome};
use crate::mqtt::delivery::*;
use crate::mqtt::error::*;
use crate::mqtt::state::{ProtocolPacketKind, ProtocolSessionState};
use crate::mqtt::util::*;
use crate::mqtt::writer::*;
use crate::mqtt::{InflightMessage, PublishRequest};
use crate::BrokerRuntime;
use greenmqtt_core::{Delivery, InflightPhase};
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub(crate) const MAX_PENDING_INBOUND_QOS2: usize = 256;
const WRITE_STALL_TIMEOUT: Duration = Duration::from_millis(100);
const WRITE_STALL_BACKOFF: Duration = Duration::from_millis(100);

pub(crate) struct PreparedReplayPacket {
    packet: Vec<u8>,
    packet_id: Option<u16>,
    delivery: Option<Delivery>,
    inflight: Option<InflightMessage>,
}

pub(crate) async fn drive_session<T, A, C, H>(
    mut stream: T,
    broker: Arc<BrokerRuntime<A, C, H>>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut will_publish: Option<PublishRequest> = None;
    let mut local_session_epoch = None;
    let mut receive_maximum = usize::from(u16::MAX);
    let mut send_quota = SendQuotaManager::new(receive_maximum);
    let mut include_problem_information = true;
    let mut client_max_packet_size = None;
    let mut keep_alive = None;
    let mut last_activity = Instant::now();
    let mut protocol_level = 4u8;
    let mut pending_inbound_qos2 = HashSet::new();
    let mut outbound_qos1 = HashMap::<u16, Delivery>::new();
    let mut outbound_qos2_publish = HashMap::<u16, Delivery>::new();
    let mut outbound_qos2_release = HashMap::<u16, Delivery>::new();
    let mut next_outbound_packet_id = 1u16;
    let mut protocol_session_state = ProtocolSessionState::default();
    let mut push_paused_until = None;
    let mut write_stall_count = 0usize;
    let mut write_stall_duration = Duration::ZERO;
    let result = 'session: loop {
        tokio::select! {
            packet = stream.read_packet(protocol_level, broker.config.max_packet_size) => {
                let packet = match packet {
                    Ok(Some(packet)) => packet,
                    Ok(None) => break 'session Ok(()),
                    Err(error) => {
                        let _ = write_protocol_read_error_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            protocol_error_disconnect_reason_string(&error),
                            broker.config.server_reference.as_deref(),
                            )
                        .await;
                        break 'session Err(error);
                    }
                };
                last_activity = Instant::now();
                if !matches!(packet, Packet::Connect(_)) {
                    if let Err(error) = require_current_session_or_disconnect(
                        &mut stream,
                        broker.as_ref(),
                        &mut protocol_session_state,
                        local_session_epoch,
                        protocol_level,
                        include_problem_information,
                        broker.config.server_reference.as_deref(),
                    )
                    .await
                    {
                        break 'session Err(error);
                    }
                }
                match packet {
                    Packet::Connect(connect) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::Connect,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        let request_problem_information =
                            connect.request_problem_information.unwrap_or(true);
                        protocol_level = connect.protocol_level;
                        let prepared = match prepare_connect(broker.as_ref(), connect) {
                            Ok(prepared) => prepared,
                            Err(error) => {
                                let packet = build_connack_error_packet(
                                    protocol_level,
                                    connect_error_reason_code(protocol_level, &error)
                                        .unwrap_or(0x82),
                                    request_problem_information
                                        .then(|| connect_error_reason_string(&error))
                                        .flatten(),
                                    broker.config.server_reference.as_deref(),
                                );
                                let _ = stream.write_bytes(&packet).await;
                                break 'session Err(error);
                            }
                        };
                        protocol_level = prepared.protocol_level;
                        will_publish = prepared.will_publish.clone();
                        receive_maximum = prepared.receive_maximum;
                        include_problem_information = prepared.include_problem_information;
                        client_max_packet_size = prepared.client_max_packet_size;
                        keep_alive = negotiated_keep_alive(
                            prepared.keep_alive_secs,
                            broker.config.server_keep_alive_secs,
                        );
                        if broker.connect_pressure_exceeded() {
                            let packet = build_connack_error_packet(
                                protocol_level,
                                if protocol_level == 5 { 0x89 } else { 0x03 },
                                prepared
                                    .include_problem_information
                                    .then_some("server busy"),
                                broker.config.server_reference.as_deref(),
                            );
                            let _ = stream.write_bytes(&packet).await;
                            break 'session Ok(());
                        }
                        if broker.connect_debounce_exceeded(&prepared.connect_request.identity) {
                            let packet = build_connack_error_packet(
                                protocol_level,
                                if protocol_level == 5 { 0x89 } else { 0x03 },
                                prepared
                                    .include_problem_information
                                    .then_some("connect debounce active"),
                                broker.config.server_reference.as_deref(),
                            );
                            let _ = stream.write_bytes(&packet).await;
                            break 'session Ok(());
                        }
                        if let Some(delay) = broker.connect_effective_delay() {
                            sleep(delay).await;
                        }
                        let mut enhanced_auth_completed = false;
                        if protocol_level == 5 {
                            if let Some(auth_method) = prepared.auth_method.as_deref() {
                                match drive_connect_enhanced_auth(
                                    &mut stream,
                                    broker.as_ref(),
                                    &prepared.connect_request.identity,
                                    auth_method,
                                    prepared.auth_data.as_deref(),
                                    include_problem_information,
                                    client_max_packet_size,
                                )
                                .await
                                {
                                    Ok(ConnectAuthOutcome::Complete) => {}
                                    Ok(ConnectAuthOutcome::Disconnect) => {
                                        break 'session Ok(());
                                    }
                                    Err(error) => {
                                        if let Some(reason_code) =
                                            connect_error_reason_code(protocol_level, &error)
                                        {
                                            let packet = build_connack_error_packet(
                                                protocol_level,
                                                reason_code,
                                                prepared
                                                    .include_problem_information
                                                    .then(|| connect_error_reason_string(&error))
                                                    .flatten(),
                                                broker.config.server_reference.as_deref(),
                                            );
                                            let _ = stream.write_bytes(&packet).await;
                                            break 'session Ok(());
                                        }
                                        break 'session Err(error);
                                    }
                                }
                                enhanced_auth_completed = true;
                            }
                        }
                        let connect_request = prepared.connect_request;
                        if let Some(redirection) =
                            broker.client_balancer.need_redirect(&connect_request.identity)
                        {
                            let packet = build_connack_error_packet(
                                protocol_level,
                                if protocol_level == 5 {
                                    if redirection.permanent { 0x9D } else { 0x9C }
                                } else {
                                    0x03
                                },
                                prepared
                                    .include_problem_information
                                    .then_some(if redirection.permanent {
                                        "server moved"
                                    } else {
                                        "use another server"
                                    }),
                                Some(&redirection.server_reference),
                            );
                            let _ = stream.write_bytes(&packet).await;
                            break 'session Ok(());
                        }
                        let reply = match if enhanced_auth_completed {
                            broker
                                .connect_authenticated_without_replay(connect_request)
                                .await
                        } else {
                            broker.connect_without_replay(connect_request).await
                        } {
                            Ok(reply) => reply,
                            Err(error) => {
                                if let Some(reason_code) =
                                    connect_error_reason_code(protocol_level, &error)
                                {
                                    let packet = build_connack_error_packet(
                                        protocol_level,
                                        reason_code,
                                        prepared
                                            .include_problem_information
                                            .then(|| connect_error_reason_string(&error))
                                            .flatten(),
                                        broker.config.server_reference.as_deref(),
                                    );
                                    let _ = stream.write_bytes(&packet).await;
                                    break 'session Ok(());
                                }
                                break 'session Err(error);
                            }
                        };
                        let active_session = reply.session.session_id.clone();
                        local_session_epoch = Some(reply.local_session_epoch);
                        protocol_session_state.on_connected_with_lifecycle(
                            active_session.clone(),
                            reply.session.identity.clone(),
                            prepared.auth_method,
                            prepared.protocol_level,
                            prepared.receive_maximum,
                            prepared.include_problem_information,
                            prepared.client_max_packet_size,
                            prepared.current_session_expiry_interval,
                            prepared.will_delay_interval_secs,
                            will_publish.is_some(),
                        );
                        receive_maximum = protocol_session_state
                            .receive_maximum()
                            .unwrap_or(receive_maximum);
                        send_quota.set_max_quota(receive_maximum);
                        include_problem_information = protocol_session_state
                            .include_problem_information()
                            .unwrap_or(include_problem_information);
                        client_max_packet_size = protocol_session_state
                            .client_max_packet_size()
                            .or(client_max_packet_size);
                        let mut deferred_replay_deliveries = Vec::new();
                        let connack_packet = build_connack_packet_with_properties(
                            protocol_level,
                            reply.session_present,
                            &prepared.connack_properties,
                        );
                        if packet_exceeds_limit(connack_packet.len(), client_max_packet_size) {
                            break 'session Err(anyhow::anyhow!("connack exceeds client maximum packet size"));
                        }
                        if let Err(error) = stream.write_bytes(&connack_packet).await {
                            break 'session Err(error);
                        }
                        let (replay_offline_messages, replay_inflight_messages) =
                            match broker.load_session_replay(&active_session).await {
                                Ok(replay) => replay,
                                Err(error) => break 'session Err(error),
                            };
                        let mut replay_packets = Vec::new();
                        let mut staged_packet_ids = VecDeque::new();
                        for offline in replay_offline_messages {
                            if offline.qos > 0
                                && outbound_qos1.len()
                                    + outbound_qos2_publish.len()
                                    + outbound_qos2_release.len()
                                    + staged_packet_ids.len()
                                    >= send_quota.current_quota()
                            {
                                deferred_replay_deliveries.push(delivery_from_offline(&offline));
                                continue;
                            }
                            let packet_id = if offline.qos > 0 {
                                let allocated = next_outbound_packet_id;
                                next_outbound_packet_id = next_packet_id(next_outbound_packet_id);
                                Some(allocated)
                            } else {
                                None
                            };
                            let packet = build_delivery_packet(
                                protocol_level,
                                &offline.topic,
                                &offline.payload,
                                offline.qos,
                                offline.retain,
                                &offline.properties,
                                packet_id,
                            )?;
                            if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                                let _ = write_packet_too_large_disconnect_with_state(
                                    &mut stream,
                                    &mut protocol_session_state,
                                    protocol_level,
                                    include_problem_information,
                                    broker.config.server_reference.as_deref(),
                                )
                                .await;
                                break 'session Err(anyhow::anyhow!(
                                    "offline replay exceeds client maximum packet size"
                                ));
                            }
                            if let Some(allocated) = packet_id {
                                let delivery = delivery_from_offline(&offline);
                                staged_packet_ids.push_back(allocated);
                                replay_packets.push(PreparedReplayPacket {
                                    packet,
                                    packet_id: Some(allocated),
                                    delivery: Some(delivery.clone()),
                                    inflight: Some(inflight_from_offline(
                                        &offline,
                                        allocated,
                                        InflightPhase::Publish,
                                    )),
                                });
                            } else {
                                replay_packets.push(PreparedReplayPacket {
                                    packet,
                                    packet_id: None,
                                    delivery: None,
                                    inflight: None,
                                });
                            }
                        }
                        let inflight_batch: Vec<_> = replay_packets
                            .iter()
                            .filter_map(|packet| packet.inflight.clone())
                            .collect();
                        if !inflight_batch.is_empty() {
                            broker.inbox.stage_inflight_batch(inflight_batch).await?;
                        }
                        for replay_packet in replay_packets {
                            if let Err(error) =
                                stream.write_bytes(&replay_packet.packet).await
                            {
                                if !staged_packet_ids.is_empty() {
                                    let rollback_packet_ids: Vec<_> =
                                        staged_packet_ids.into_iter().collect();
                                    if let Err(rollback_error) = broker
                                        .inbox
                                        .ack_inflight_batch(&active_session, &rollback_packet_ids)
                                        .await
                                    {
                                        break 'session Err(anyhow::anyhow!(
                                            "failed to send replay packet: {error}; failed to rollback staged inflight: {rollback_error}"
                                        ));
                                    }
                                }
                                break 'session Err(error);
                            }
                            if let Some(packet_id) = replay_packet.packet_id {
                                debug_assert_eq!(staged_packet_ids.pop_front(), Some(packet_id));
                            }
                            if let Some(delivery) = replay_packet.delivery {
                                if delivery.qos == 1 {
                                    outbound_qos1.insert(
                                        replay_packet
                                            .packet_id
                                            .expect("qos delivery must have packet id"),
                                        delivery,
                                    );
                                } else if delivery.qos == 2 {
                                    outbound_qos2_publish.insert(
                                        replay_packet
                                            .packet_id
                                            .expect("qos delivery must have packet id"),
                                        delivery,
                                    );
                                }
                            }
                        }
                        if !deferred_replay_deliveries.is_empty() {
                            broker
                                .requeue_local_deliveries(
                                    &active_session,
                                    deferred_replay_deliveries,
                                )
                                .await?;
                        }
                        for inflight in replay_inflight_messages {
                            match inflight.phase {
                                InflightPhase::Publish => {
                                    let delivery = delivery_from_inflight(&inflight);
                                    let egress_tenant_id = delivery.tenant_id.clone();
                                    let egress_qos = delivery.qos;
                                    let egress_payload_bytes = delivery.payload.len();
                                    if inflight.qos == 1 {
                                        outbound_qos1.insert(inflight.packet_id, delivery);
                                    } else if inflight.qos == 2 {
                                        outbound_qos2_publish.insert(inflight.packet_id, delivery);
                                    }
                                    let packet = build_delivery_packet(
                                        protocol_level,
                                        &inflight.topic,
                                        &inflight.payload,
                                        inflight.qos,
                                        inflight.retain,
                                        &inflight.properties,
                                        Some(inflight.packet_id),
                                    )?;
                                    if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                                        let _ = write_packet_too_large_disconnect_with_state(
                                            &mut stream,
                                            &mut protocol_session_state,
                                            protocol_level,
                                            include_problem_information,
                                            broker.config.server_reference.as_deref(),
                                        )
                                        .await;
                                        break 'session Err(anyhow::anyhow!(
                                            "inflight replay exceeds client maximum packet size"
                                        ));
                                    }
                                    if let Err(error) = stream.write_bytes(&packet).await {
                                        break 'session Err(error);
                                    }
                                    BrokerMetrics::record_publish_egress(
                                        &egress_tenant_id,
                                        egress_qos,
                                        egress_payload_bytes,
                                    );
                                }
                                InflightPhase::Release => {
                                    if inflight.qos == 2 {
                                        outbound_qos2_release
                                            .insert(inflight.packet_id, delivery_from_inflight(&inflight));
                                        let packet = build_pubrel_packet(protocol_level, inflight.packet_id);
                                        if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                                            let _ = write_packet_too_large_disconnect_with_state(
                                                &mut stream,
                                                &mut protocol_session_state,
                                                protocol_level,
                                                include_problem_information,
                                                broker.config.server_reference.as_deref(),
                                            )
                                            .await;
                                            break 'session Err(anyhow::anyhow!(
                                                "pubrel replay exceeds client maximum packet size"
                                            ));
                                        }
                                        if let Err(error) = stream.write_bytes(&packet).await {
                                            break 'session Err(error);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Packet::Subscribe(subscribe) => {
                        let active_session = match require_packet_session_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::Subscribe,
                        )
                        .await
                        {
                            Ok(active_session) => active_session,
                            Err(error) => break 'session Err(error),
                        };
                        let mut return_codes = Vec::new();
                        let mut retained_deliveries = Vec::new();
                        for item in subscribe.subscriptions {
                            let (shared_group, topic_filter) = split_shared_subscription(&item.topic_filter);
                            if protocol_level == 5 && shared_group.is_some() && item.no_local {
                                protocol_session_state.begin_disconnect(None);
            let state_error = ProtocolStateError {
                                        reason_code: 0x82,
                                        reason_string: "invalid subscribe options",
                                    };
            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
            let error =  anyhow::anyhow!(state_error.reason_string);

                                break 'session Err(error);
                            }
                            match broker
                                .subscribe(
                                    &active_session,
                                    &topic_filter,
                                    item.qos,
                                    item.subscription_identifier,
                                    item.no_local,
                                    item.retain_as_published,
                                    item.retain_handling,
                                    shared_group,
                                )
                                .await
                            {
                                Ok(retained) => {
                                    retained_deliveries.push((
                                        retained,
                                        item.qos,
                                        item.retain_as_published,
                                        item.subscription_identifier,
                                    ));
                                    return_codes.push(item.qos);
                                }
                                    Err(error) => {
                                    if let Some(return_code) =
                                        subscribe_error_return_code(protocol_level, &error)
                                    {
                                        return_codes.push(return_code);
                                    } else {
                                        break 'session Err(error);
                                    }
                                }
                            }
                        }
                        if let Err(error) = write_suback(
                            &mut stream,
                            protocol_level,
                            subscribe.packet_id,
                            &return_codes,
                            include_problem_information
                                .then_some(())
                                .and_then(|_| {
                                    return_codes
                                        .iter()
                                        .any(|code| *code >= 0x80)
                                        .then_some("subscribe denied")
                                }),
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        for (retained_messages, subscription_qos, retain_as_published, subscription_identifier) in retained_deliveries {
                            for retained in retained_messages {
                                let qos = retained.qos.min(subscription_qos);
                                let packet_id = if qos > 0 {
                                    let allocated = next_outbound_packet_id;
                                    next_outbound_packet_id = next_packet_id(next_outbound_packet_id);
                                    Some(allocated)
                                } else {
                                    None
                                };
                                let packet = build_delivery_packet(
                                    protocol_level,
                                    &retained.topic,
                                    &retained.payload,
                                    qos,
                                    retain_as_published,
                                    &delivery_properties_with_subscription_identifier(
                                        subscription_identifier,
                                    ),
                                    packet_id,
                                )?;
                                if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                                    let _ = write_packet_too_large_disconnect_with_state(
                                        &mut stream,
                                        &mut protocol_session_state,
                                        protocol_level,
                                        include_problem_information,
                                        broker.config.server_reference.as_deref(),
                                    )
                                    .await;
                                    break 'session Err(anyhow::anyhow!(
                                        "retained replay exceeds client maximum packet size"
                                    ));
                                }
                                if let Some(allocated) = packet_id {
                                    let delivery = Delivery {
                                        tenant_id: retained.tenant_id.clone(),
                                        session_id: active_session.clone(),
                                        topic: retained.topic.clone(),
                                        payload: retained.payload.clone(),
                                        qos,
                                        retain: retain_as_published,
                                        from_session_id: String::new(),
                                        properties: delivery_properties_with_subscription_identifier(
                                            subscription_identifier,
                                        ),
                                    };
                                    broker
                                        .inbox
                                        .stage_inflight(inflight_from_delivery(
                                            &delivery,
                                            allocated,
                                            InflightPhase::Publish,
                                        ))
                                        .await?;
                                    if qos == 1 {
                                        outbound_qos1.insert(allocated, delivery);
                                    } else if qos == 2 {
                                        outbound_qos2_publish.insert(allocated, delivery);
                                    }
                                }
                                if let Err(error) = stream.write_bytes(&packet).await {
                                    break 'session Err(error);
                                }
                            }
                        }
                    }
                    Packet::Unsubscribe(unsubscribe) => {
                        let active_session = match require_packet_session_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::Unsubscribe,
                        )
                        .await
                        {
                            Ok(active_session) => active_session,
                            Err(error) => break 'session Err(error),
                        };
                        let mut return_codes = Vec::with_capacity(unsubscribe.topic_filters.len());
                        for topic_filter in unsubscribe.topic_filters {
                            let (shared_group, topic_filter) = split_shared_subscription(&topic_filter);
                            let removed = broker
                                .unsubscribe_with_group(&active_session, &topic_filter, shared_group)
                                .await?;
                            return_codes.push(if removed { 0x00 } else { 0x11 });
                        }
                        if let Err(error) =
                            write_unsuback(
                                &mut stream,
                                protocol_level,
                                unsubscribe.packet_id,
                                &return_codes,
                            )
                            .await
                        {
                            break 'session Err(error);
                        }
                    }
                    Packet::Publish(publish) => {
                        let active_session = match require_packet_session_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::Publish,
                        )
                        .await
                        {
                            Ok(active_session) => active_session,
                            Err(error) => break 'session Err(error),
                        };
                        let topic = match protocol_session_state.resolve_publish_topic(
                            publish.topic_alias,
                            &publish.topic,
                        ) {
                            Ok(topic) => topic,
                            Err(_error) => {
                                protocol_session_state.begin_disconnect(None);
            let state_error = ProtocolStateError {
                                        reason_code: 0x82,
                                        reason_string: "invalid topic alias",
                                    };
            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
            let error =  anyhow::anyhow!(state_error.reason_string);

                                break 'session Err(error);
                            }
                        };
                        let publish_request = PublishRequest {
                            topic,
                            payload: publish.payload.into(),
                            qos: publish.qos,
                            retain: publish.retain,
                            properties: publish.properties,
                        };
                        if let Err(error) = validate_utf8_payload_format(
                            &publish_request.properties,
                            &publish_request.payload,
                        ) {
                            if let Some(reason_code) =
                                publish_error_reason_code(protocol_level, &error)
                            {
                                if publish.qos == 1 {
                                    if let Some(packet_id) = publish.packet_id {
                                        if let Err(write_error) = write_puback(
                                            &mut stream,
                                            protocol_level,
                                            packet_id,
                                            Some(reason_code),
                                            include_problem_information
                                                .then(|| publish_error_reason_string(&error))
                                                .flatten(),
                                        )
                                        .await
                                        {
                                            break 'session Err(write_error);
                                        }
                                        continue;
                                    }
                                } else {
                                    protocol_session_state.begin_disconnect(None);
                                    let state_error = ProtocolStateError {
                                        reason_code,
                                        reason_string: publish_error_reason_string(&error)
                                            .unwrap_or("payload format invalid"),
                                    };
                                    let _ = write_protocol_state_error_disconnect_with_context(
                                        &mut stream,
                                        protocol_level,
                                        include_problem_information,
                                        state_error,
                                        broker.config.server_reference.as_deref(),
                                    )
                                    .await;
                                    break 'session Err(error);
                                }
                            }
                            break 'session Err(error);
                        }
                        let publish_result = if broker.allow_publish_rate_for_session(&active_session)
                        {
                            broker.publish(&active_session, publish_request).await
                        } else {
                            Err(anyhow::anyhow!("publish rate exceeded"))
                        };
                        if let Err(error) = publish_result {
                            if let Some(reason_code) =
                                publish_error_reason_code(protocol_level, &error)
                            {
                                if publish.qos == 1 {
                                    if let Some(packet_id) = publish.packet_id {
                                        if let Err(write_error) =
                                            write_puback(
                                                &mut stream,
                                                protocol_level,
                                                packet_id,
                                                Some(reason_code),
                                                include_problem_information
                                                    .then(|| publish_error_reason_string(&error))
                                                    .flatten(),
                                            ).await
                                        {
                                            break 'session Err(write_error);
                                        }
                                        continue;
                                    }
                                } else if publish.qos == 2 {
                                    if let Some(packet_id) = publish.packet_id {
                                        if let Err(write_error) =
                                            write_pubrec(
                                                &mut stream,
                                                protocol_level,
                                                packet_id,
                                                Some(reason_code),
                                                include_problem_information
                                                    .then(|| publish_error_reason_string(&error))
                                                    .flatten(),
                                            ).await
                                        {
                                            break 'session Err(write_error);
                                        }
                                        continue;
                                    }
                                }
                            }
                            break 'session Err(error);
                        }
                        if publish.qos == 1 {
                            if let Some(packet_id) = publish.packet_id {
                                    if let Err(error) = write_puback(
                                        &mut stream,
                                        protocol_level,
                                        packet_id,
                                        None,
                                        None,
                                    )
                                    .await
                                    {
                                        break 'session Err(error);
                                    }
                            }
                        } else if publish.qos == 2 {
                            if let Some(packet_id) = publish.packet_id {
                                if !pending_inbound_qos2.contains(&packet_id)
                                    && pending_inbound_qos2.len() >= MAX_PENDING_INBOUND_QOS2
                                {
                                    protocol_session_state.begin_disconnect(None);
            let state_error = ProtocolStateError {
                                            reason_code: 0x97,
                                            reason_string: "too many pending qos2 publishes",
                                        };
            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
            let error =  anyhow::anyhow!(state_error.reason_string);

                                    break 'session Err(error);
                                }
                                pending_inbound_qos2.insert(packet_id);
                                    if let Err(error) = write_pubrec(
                                        &mut stream,
                                        protocol_level,
                                        packet_id,
                                        None,
                                        None,
                                    )
                                    .await
                                    {
                                        break 'session Err(error);
                                    }
                            }
                        }
                    }
                    Packet::PubAck(packet_id) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubAck,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        if let Some(delivery) = outbound_qos1.remove(&packet_id) {
                            if let Some(stored_at_ms) = delivery.properties.stored_at_ms {
                                let latency_seconds =
                                    current_millis().saturating_sub(stored_at_ms) as f64 / 1000.0;
                                BrokerMetrics::record_qos_latency(1, latency_seconds);
                            }
                        }
                        send_quota.note_ack();
                        let active_session = match require_packet_session_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubAck,
                        )
                        .await
                        {
                            Ok(active_session) => active_session,
                            Err(error) => break 'session Err(error),
                        };
                        broker.inbox.ack_inflight(&active_session, packet_id).await?;
                    }
                    Packet::PubRec(packet_id) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubRec,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        if let Some(delivery) = outbound_qos2_publish.remove(&packet_id) {
                            send_quota.note_ack();
                            broker
                                .inbox
                                .stage_inflight(
                                    inflight_from_delivery(
                                        &delivery,
                                        packet_id,
                                        InflightPhase::Release,
                                    ),
                                )
                                .await?;
                            outbound_qos2_release.insert(packet_id, delivery);
                            if let Err(error) =
                                write_pubrel(&mut stream, protocol_level, packet_id).await
                            {
                                break 'session Err(error);
                            }
                        } else {
                            protocol_session_state.begin_disconnect(None);
                            let state_error = ProtocolStateError {
                                reason_code: 0x82,
                                reason_string: "unexpected pubrec packet id",
                            };
                            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
                            break 'session Err(anyhow::anyhow!(state_error.reason_string));
                        }
                    }
                    Packet::PubRel(packet_id) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubRel,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        if !pending_inbound_qos2.remove(&packet_id) {
                            protocol_session_state.begin_disconnect(None);
                            let state_error = ProtocolStateError {
                                reason_code: 0x82,
                                reason_string: "unexpected pubrel packet id",
                            };
                            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
                            break 'session Err(anyhow::anyhow!(state_error.reason_string));
                        }
                        if let Err(error) =
                            write_pubcomp(&mut stream, protocol_level, packet_id).await
                        {
                            break 'session Err(error);
                        }
                    }
                    Packet::PubComp(packet_id) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubComp,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        let Some(delivery) = outbound_qos2_release.remove(&packet_id) else {
                            protocol_session_state.begin_disconnect(None);
                            let state_error = ProtocolStateError {
                                reason_code: 0x82,
                                reason_string: "unexpected pubcomp packet id",
                            };
                            let _ = write_protocol_state_error_disconnect_with_context(&mut stream, protocol_level, include_problem_information, state_error, broker.config.server_reference.as_deref()).await;
                            break 'session Err(anyhow::anyhow!(state_error.reason_string));
                        };
                        if let Some(stored_at_ms) = delivery.properties.stored_at_ms {
                            let latency_seconds =
                                current_millis().saturating_sub(stored_at_ms) as f64 / 1000.0;
                            BrokerMetrics::record_qos_latency(2, latency_seconds);
                        }
                        send_quota.note_ack();
                        let active_session = match require_packet_session_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PubComp,
                        )
                        .await
                        {
                            Ok(active_session) => active_session,
                            Err(error) => break 'session Err(error),
                        };
                        broker.inbox.ack_inflight(&active_session, packet_id).await?;
                    }
                    Packet::PingReq => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::PingReq,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        if let Err(error) = write_pingresp(&mut stream).await {
                            break 'session Err(error);
                        }
                    }
                    Packet::Disconnect(disconnect) => {
                        if let Err(error) = require_packet_gate_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                            ProtocolPacketKind::Disconnect,
                        )
                        .await
                        {
                            break 'session Err(error);
                        }
                        protocol_session_state.begin_disconnect(disconnect.session_expiry_interval);
                        break 'session Ok(());
                    }
                    Packet::Auth(auth) => {
                        let reauth = match start_session_reauth_or_disconnect(
                            &mut stream,
                            &mut protocol_session_state,
                            &auth,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                        )
                        .await
                        {
                            Ok(reauth) => reauth,
                            Err(error) => break 'session Err(error),
                        };
                        match drive_session_reauth(
                            &mut stream,
                            broker.as_ref(),
                            &reauth.identity,
                            &reauth.auth_method,
                            auth.auth_data.as_deref(),
                            include_problem_information,
                            client_max_packet_size,
                        )
                        .await?
                        {
                            ConnectAuthOutcome::Complete => {
                                protocol_session_state
                                    .complete_session_reauth(reauth.auth_method);
                            }
                            ConnectAuthOutcome::Disconnect => break 'session Ok(()),
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)), if protocol_session_state.has_active_session() => {
                if keep_alive_timed_out(last_activity, keep_alive) {
                    break 'session Err(anyhow::anyhow!("keep alive timeout"));
                }
                if push_paused_until.is_some_and(|until: Instant| Instant::now() < until) {
                    continue;
                }
                let active_session = protocol_session_state
                    .active_session_id_cloned()
                    .expect("checked above");
                let deliveries = broker.drain_deliveries(&active_session).await?;
                let mut deferred = Vec::new();
                let mut deliveries = deliveries.into_iter();
                while let Some(delivery) = deliveries.next() {
                    if delivery.qos > 0
                        && outbound_qos1.len() + outbound_qos2_publish.len() + outbound_qos2_release.len()
                            >= send_quota.current_quota()
                    {
                        deferred.push(delivery);
                        continue;
                    }
                    let packet_id = if delivery.qos > 0 {
                        let allocated = next_outbound_packet_id;
                        next_outbound_packet_id = next_packet_id(next_outbound_packet_id);
                        Some(allocated)
                    } else {
                        None
                    };
                    let packet = build_delivery_packet(
                        protocol_level,
                        &delivery.topic,
                        &delivery.payload,
                        delivery.qos,
                        delivery.retain,
                        &delivery.properties,
                        packet_id,
                    )?;
                    if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                        let _ = write_packet_too_large_disconnect_with_state(
                            &mut stream,
                            &mut protocol_session_state,
                            protocol_level,
                            include_problem_information,
                            broker.config.server_reference.as_deref(),
                        )
                        .await;
                        break 'session Err(anyhow::anyhow!(
                            "live delivery exceeds client maximum packet size"
                        ));
                    }
                    if let Some(allocated) = packet_id {
                        broker
                            .inbox
                            .stage_inflight(
                                inflight_from_delivery(
                                    &delivery,
                                    allocated,
                                    InflightPhase::Publish,
                                ),
                            )
                            .await?;
                        if delivery.qos == 1 {
                            outbound_qos1.insert(allocated, delivery.clone());
                        } else if delivery.qos == 2 {
                            outbound_qos2_publish.insert(allocated, delivery.clone());
                        }
                    }
                    match timeout(WRITE_STALL_TIMEOUT, stream.write_bytes(&packet)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => break 'session Err(error),
                        Err(_) => {
                            write_stall_count += 1;
                            write_stall_duration += WRITE_STALL_TIMEOUT;
                            push_paused_until = Some(Instant::now() + WRITE_STALL_BACKOFF);
                            deferred.push(delivery);
                            deferred.extend(deliveries);
                            break;
                        }
                    }
                }
                if !deferred.is_empty() {
                    broker.requeue_local_deliveries(&active_session, deferred).await?;
                }
            }
        }
    };

    let publish_will = protocol_session_state.publish_will().unwrap_or(false);
    let disconnect_session_expiry_interval =
        protocol_session_state.disconnect_session_expiry_interval();
    let effective_will_delay_secs = protocol_session_state.effective_will_delay_interval_secs();
    if let Some(active_session) = protocol_session_state.take_session_id() {
        if publish_will {
            if let Some(will) = will_publish.take() {
                if let Some(delay_secs) = effective_will_delay_secs.filter(|delay| *delay > 0) {
                    if let Err(error) = broker
                        .schedule_delayed_will(&active_session, will, delay_secs)
                        .await
                    {
                        eprintln!("greenmqtt will schedule error: {error:#}");
                    }
                } else if let Err(error) = broker.publish(&active_session, will).await {
                    eprintln!("greenmqtt will publish error: {error:#}");
                }
            }
        }
        let cleanup = if let Some(session_epoch) = local_session_epoch {
            broker
                .disconnect_current_session(
                    &active_session,
                    session_epoch,
                    disconnect_session_expiry_interval,
                )
                .await
        } else {
            match disconnect_session_expiry_interval {
                Some(expiry_secs) => broker
                    .disconnect_with_session_expiry(&active_session, expiry_secs)
                    .await
                    .map(|_| true),
                None => broker.disconnect(&active_session).await.map(|_| true),
            }
        };
        if let Err(error) = cleanup {
            eprintln!("greenmqtt session cleanup error: {error:#}");
        }
    }
    if write_stall_count > 0 {
        eprintln!(
            "greenmqtt session write stalls: count={}, total_ms={}",
            write_stall_count,
            write_stall_duration.as_millis()
        );
    }
    result
}
