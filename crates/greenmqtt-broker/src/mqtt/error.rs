use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};

use super::connect;
use super::connect::build_disconnect_packet_with_server_reference;
use super::session::SessionTransport;
use super::state;
use super::state::{ProtocolPacketKind, ProtocolSessionState, ProtocolStateError};
use super::BrokerRuntime;

pub(crate) async fn write_packet_too_large_disconnect<T>(
    stream: &mut T,
    protocol_level: u8,
    include_problem_information: bool,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    if protocol_level == 5 {
        stream
            .write_bytes(&build_disconnect_packet_with_server_reference(
                protocol_level,
                Some(0x95),
                include_problem_information.then_some("packet too large"),
                server_reference,
            ))
            .await?;
    }
    Ok(())
}

pub(crate) async fn write_packet_too_large_disconnect_with_state<T>(
    stream: &mut T,
    protocol_session_state: &mut ProtocolSessionState,
    fallback_protocol_level: u8,
    fallback_include_problem_information: bool,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    let protocol_level = protocol_session_state
        .protocol_level()
        .unwrap_or(fallback_protocol_level);
    let include_problem_information = protocol_session_state
        .include_problem_information()
        .unwrap_or(fallback_include_problem_information);
    protocol_session_state.begin_disconnect(None);
    write_packet_too_large_disconnect(
        stream,
        protocol_level,
        include_problem_information,
        server_reference,
    )
    .await
}

pub(crate) async fn write_protocol_error_disconnect<T>(
    stream: &mut T,
    protocol_level: u8,
    include_problem_information: bool,
    reason_string: Option<&str>,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    if protocol_level == 5 {
        stream
            .write_bytes(&build_disconnect_packet_with_server_reference(
                protocol_level,
                Some(0x82),
                include_problem_information
                    .then_some(reason_string)
                    .flatten(),
                server_reference,
            ))
            .await?;
    }
    Ok(())
}

pub(crate) async fn write_protocol_state_error_disconnect_with_context<T>(
    stream: &mut T,
    protocol_level: u8,
    include_problem_information: bool,
    error: ProtocolStateError,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    if error.reason_code == 0x82 {
        write_protocol_error_disconnect(
            stream,
            protocol_level,
            include_problem_information,
            Some(error.reason_string),
            server_reference,
        )
        .await
    } else if protocol_level == 5 {
        stream
            .write_bytes(&build_disconnect_packet_with_server_reference(
                protocol_level,
                Some(error.reason_code),
                include_problem_information.then_some(error.reason_string),
                server_reference,
            ))
            .await
    } else {
        Ok(())
    }
}

pub(crate) async fn write_protocol_state_error_disconnect<T>(
    stream: &mut T,
    protocol_session_state: &ProtocolSessionState,
    protocol_level: u8,
    include_problem_information: bool,
    error: ProtocolStateError,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    let protocol_level = protocol_session_state
        .protocol_level()
        .unwrap_or(protocol_level);
    let include_problem_information = protocol_session_state
        .include_problem_information()
        .unwrap_or(include_problem_information);
    write_protocol_state_error_disconnect_with_context(
        stream,
        protocol_level,
        include_problem_information,
        error,
        server_reference,
    )
    .await
}

pub(crate) async fn write_protocol_read_error_disconnect<T>(
    stream: &mut T,
    protocol_session_state: &mut ProtocolSessionState,
    fallback_protocol_level: u8,
    fallback_include_problem_information: bool,
    reason_string: Option<&'static str>,
    server_reference: Option<&str>,
) -> anyhow::Result<()>
where
    T: SessionTransport,
{
    let Some(reason_string) = reason_string else {
        return Ok(());
    };
    let protocol_level = protocol_session_state
        .protocol_level()
        .unwrap_or(fallback_protocol_level);
    let include_problem_information = protocol_session_state
        .include_problem_information()
        .unwrap_or(fallback_include_problem_information);
    protocol_session_state.begin_disconnect(None);
    write_protocol_error_disconnect(
        stream,
        protocol_level,
        include_problem_information,
        Some(reason_string),
        server_reference,
    )
    .await
}

pub(crate) async fn require_packet_gate_or_disconnect<T>(
    stream: &mut T,
    protocol_session_state: &mut ProtocolSessionState,
    protocol_level: u8,
    include_problem_information: bool,
    server_reference: Option<&str>,
    kind: ProtocolPacketKind,
) -> Result<(), anyhow::Error>
where
    T: SessionTransport,
{
    match protocol_session_state.prepare_packet(kind) {
        Ok(()) => Ok(()),
        Err(error) => {
            protocol_session_state.begin_disconnect(None);
            let _ = write_protocol_state_error_disconnect(
                stream,
                protocol_session_state,
                protocol_level,
                include_problem_information,
                error,
                server_reference,
            )
            .await;
            Err(anyhow::anyhow!(error.reason_string))
        }
    }
}

pub(crate) async fn require_packet_session_or_disconnect<T>(
    stream: &mut T,
    protocol_session_state: &mut ProtocolSessionState,
    protocol_level: u8,
    include_problem_information: bool,
    server_reference: Option<&str>,
    kind: ProtocolPacketKind,
) -> Result<String, anyhow::Error>
where
    T: SessionTransport,
{
    match protocol_session_state.session_id_for_packet(kind) {
        Ok(active_session) => Ok(active_session),
        Err(error) => {
            protocol_session_state.begin_disconnect(None);
            let _ = write_protocol_state_error_disconnect(
                stream,
                protocol_session_state,
                protocol_level,
                include_problem_information,
                error,
                server_reference,
            )
            .await;
            Err(anyhow::anyhow!(error.reason_string))
        }
    }
}

pub(crate) async fn require_current_session_or_disconnect<T, A, C, H>(
    stream: &mut T,
    broker: &BrokerRuntime<A, C, H>,
    protocol_session_state: &mut ProtocolSessionState,
    local_session_epoch: Option<u64>,
    protocol_level: u8,
    include_problem_information: bool,
    server_reference: Option<&str>,
) -> Result<(), anyhow::Error>
where
    T: SessionTransport,
    A: AuthProvider,
    C: AclProvider,
    H: EventHook,
{
    let Some(session_epoch) = local_session_epoch else {
        return Ok(());
    };
    let Some(active_session) = protocol_session_state.active_session_id_cloned() else {
        return Ok(());
    };
    if broker.local_session_epoch_matches(&active_session, session_epoch) {
        return Ok(());
    }
    let error = ProtocolStateError {
        reason_code: 0x8E,
        reason_string: "session taken over",
    };
    protocol_session_state.begin_disconnect(None);
    let _ = write_protocol_state_error_disconnect_with_context(
        stream,
        protocol_level,
        include_problem_information,
        error,
        server_reference,
    )
    .await;
    Err(anyhow::anyhow!(error.reason_string))
}

pub(crate) async fn start_session_reauth_or_disconnect<T>(
    stream: &mut T,
    protocol_session_state: &mut ProtocolSessionState,
    auth: &connect::AuthPacket,
    protocol_level: u8,
    include_problem_information: bool,
    server_reference: Option<&str>,
) -> Result<state::SessionReauthRequest, anyhow::Error>
where
    T: SessionTransport,
{
    match protocol_session_state.start_session_reauth(auth) {
        Ok(reauth) => Ok(reauth),
        Err(error) => {
            protocol_session_state.begin_disconnect(None);
            let _ = write_protocol_state_error_disconnect_with_context(
                stream,
                protocol_level,
                include_problem_information,
                error,
                server_reference,
            )
            .await;
            Err(anyhow::anyhow!(error.reason_string))
        }
    }
}
