use super::session::SessionTransport;
use super::{
    connect::{
        build_auth_packet, build_auth_packet_with_reason,
        build_disconnect_packet_with_server_reference,
    },
    error::{write_packet_too_large_disconnect, write_protocol_error_disconnect},
    packet_exceeds_limit, protocol_error_disconnect_reason_string, Packet,
};
use crate::BrokerRuntime;
use greenmqtt_core::ClientIdentity;
use greenmqtt_plugin_api::{AclProvider, AuthProvider, EnhancedAuthResult, EventHook};

pub(super) enum ConnectAuthOutcome {
    Complete,
    Disconnect,
}

fn session_auth_disconnect_reason_code(error: &anyhow::Error) -> u8 {
    let message = format!("{error:#}");
    if message.contains("unsupported authentication method") {
        0x8C
    } else {
        0x87
    }
}

pub(super) async fn drive_connect_enhanced_auth<T, A, C, H>(
    stream: &mut T,
    broker: &BrokerRuntime<A, C, H>,
    identity: &ClientIdentity,
    auth_method: &str,
    auth_data: Option<&[u8]>,
    include_problem_information: bool,
    client_max_packet_size: Option<u32>,
) -> anyhow::Result<ConnectAuthOutcome>
where
    T: SessionTransport,
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut result = broker
        .auth
        .begin_enhanced_auth(identity, auth_method, auth_data)
        .await?;
    loop {
        match result {
            EnhancedAuthResult::Success => return Ok(ConnectAuthOutcome::Complete),
            EnhancedAuthResult::Continue {
                auth_data,
                reason_string,
            } => {
                let packet =
                    build_auth_packet(auth_method, auth_data.as_deref(), reason_string.as_deref())?;
                if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                    write_packet_too_large_disconnect(
                        stream,
                        5,
                        include_problem_information,
                        broker.config.server_reference.as_deref(),
                    )
                    .await?;
                    return Ok(ConnectAuthOutcome::Disconnect);
                }
                stream.write_bytes(&packet).await?;
                let next = match stream.read_packet(5, broker.config.max_packet_size).await {
                    Ok(Some(packet)) => packet,
                    Ok(None) => return Ok(ConnectAuthOutcome::Disconnect),
                    Err(error) => {
                        if let Some(reason_string) = protocol_error_disconnect_reason_string(&error)
                        {
                            let _ = write_protocol_error_disconnect(
                                stream,
                                5,
                                include_problem_information,
                                Some(reason_string),
                                broker.config.server_reference.as_deref(),
                            )
                            .await;
                            return Ok(ConnectAuthOutcome::Disconnect);
                        }
                        return Err(error);
                    }
                };
                match next {
                    Packet::Auth(auth) => {
                        anyhow::ensure!(
                            matches!(auth.reason_code, 0x00 | 0x18 | 0x19),
                            "invalid auth reason code"
                        );
                        if let Some(method) = auth.auth_method.as_deref() {
                            anyhow::ensure!(
                                method == auth_method,
                                "unsupported authentication method"
                            );
                        }
                        let _ = auth.reason_string.as_deref();
                        result = broker
                            .auth
                            .continue_enhanced_auth(
                                identity,
                                auth_method,
                                auth.auth_data.as_deref(),
                            )
                            .await?;
                    }
                    Packet::Disconnect(_) => return Ok(ConnectAuthOutcome::Disconnect),
                    _ => {
                        write_protocol_error_disconnect(
                            stream,
                            5,
                            include_problem_information,
                            Some("unexpected packet before enhanced auth completion"),
                            broker.config.server_reference.as_deref(),
                        )
                        .await?;
                        return Ok(ConnectAuthOutcome::Disconnect);
                    }
                }
            }
        }
    }
}

pub(super) async fn drive_session_reauth<T, A, C, H>(
    stream: &mut T,
    broker: &BrokerRuntime<A, C, H>,
    identity: &ClientIdentity,
    auth_method: &str,
    auth_data: Option<&[u8]>,
    include_problem_information: bool,
    client_max_packet_size: Option<u32>,
) -> anyhow::Result<ConnectAuthOutcome>
where
    T: SessionTransport,
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let mut result = match broker
        .auth
        .begin_enhanced_auth(identity, auth_method, auth_data)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let reason_string = include_problem_information.then(|| format!("{error:#}"));
            stream
                .write_bytes(&build_disconnect_packet_with_server_reference(
                    5,
                    Some(session_auth_disconnect_reason_code(&error)),
                    reason_string.as_deref(),
                    broker.config.server_reference.as_deref(),
                ))
                .await?;
            return Ok(ConnectAuthOutcome::Disconnect);
        }
    };
    loop {
        match result {
            EnhancedAuthResult::Success => {
                let packet = build_auth_packet_with_reason(0x00, auth_method, None, None)?;
                if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                    write_packet_too_large_disconnect(
                        stream,
                        5,
                        include_problem_information,
                        broker.config.server_reference.as_deref(),
                    )
                    .await?;
                    return Ok(ConnectAuthOutcome::Disconnect);
                }
                stream.write_bytes(&packet).await?;
                return Ok(ConnectAuthOutcome::Complete);
            }
            EnhancedAuthResult::Continue {
                auth_data,
                reason_string,
            } => {
                let packet =
                    build_auth_packet(auth_method, auth_data.as_deref(), reason_string.as_deref())?;
                if packet_exceeds_limit(packet.len(), client_max_packet_size) {
                    write_packet_too_large_disconnect(
                        stream,
                        5,
                        include_problem_information,
                        broker.config.server_reference.as_deref(),
                    )
                    .await?;
                    return Ok(ConnectAuthOutcome::Disconnect);
                }
                stream.write_bytes(&packet).await?;
                let next = match stream.read_packet(5, broker.config.max_packet_size).await {
                    Ok(Some(packet)) => packet,
                    Ok(None) => return Ok(ConnectAuthOutcome::Disconnect),
                    Err(error) => {
                        if let Some(reason_string) = protocol_error_disconnect_reason_string(&error)
                        {
                            let _ = write_protocol_error_disconnect(
                                stream,
                                5,
                                include_problem_information,
                                Some(reason_string),
                                broker.config.server_reference.as_deref(),
                            )
                            .await;
                            return Ok(ConnectAuthOutcome::Disconnect);
                        }
                        return Err(error);
                    }
                };
                match next {
                    Packet::Auth(auth) => {
                        anyhow::ensure!(
                            matches!(auth.reason_code, 0x00 | 0x18 | 0x19),
                            "invalid auth reason code"
                        );
                        if let Some(method) = auth.auth_method.as_deref() {
                            anyhow::ensure!(
                                method == auth_method,
                                "unsupported authentication method"
                            );
                        }
                        let _ = auth.reason_string.as_deref();
                        result = match broker
                            .auth
                            .continue_enhanced_auth(
                                identity,
                                auth_method,
                                auth.auth_data.as_deref(),
                            )
                            .await
                        {
                            Ok(result) => result,
                            Err(error) => {
                                let reason_string =
                                    include_problem_information.then(|| format!("{error:#}"));
                                stream
                                    .write_bytes(&build_disconnect_packet_with_server_reference(
                                        5,
                                        Some(session_auth_disconnect_reason_code(&error)),
                                        reason_string.as_deref(),
                                        broker.config.server_reference.as_deref(),
                                    ))
                                    .await?;
                                return Ok(ConnectAuthOutcome::Disconnect);
                            }
                        };
                    }
                    Packet::Disconnect(_) => return Ok(ConnectAuthOutcome::Disconnect),
                    _ => {
                        write_protocol_error_disconnect(
                            stream,
                            5,
                            include_problem_information,
                            Some("unexpected packet during re-authentication"),
                            broker.config.server_reference.as_deref(),
                        )
                        .await?;
                        return Ok(ConnectAuthOutcome::Disconnect);
                    }
                }
            }
        }
    }
}
