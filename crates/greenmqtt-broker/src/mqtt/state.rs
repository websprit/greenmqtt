use super::connect::AuthPacket;
use greenmqtt_core::ClientIdentity;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProtocolStateError {
    pub(super) reason_code: u8,
    pub(super) reason_string: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SessionReauthRequest {
    pub(super) identity: ClientIdentity,
    pub(super) auth_method: String,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DisconnectSessionContext {
    pub(super) session_id: String,
    pub(super) current_session_expiry_interval: Option<u32>,
    pub(super) disconnect_session_expiry_interval: Option<u32>,
    pub(super) will_delay_interval_secs: Option<u32>,
    pub(super) publish_will: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProtocolPacketKind {
    Connect,
    Subscribe,
    Unsubscribe,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    PingReq,
    Disconnect,
}

#[derive(Debug)]
struct EstablishedSessionState {
    session_id: String,
    identity: ClientIdentity,
    enhanced_auth_method: Option<String>,
    protocol_level: u8,
    receive_maximum: usize,
    include_problem_information: bool,
    client_max_packet_size: Option<u32>,
    current_session_expiry_interval: Option<u32>,
    will_delay_interval_secs: Option<u32>,
    publish_will: bool,
    topic_aliases: HashMap<u16, String>,
}

#[derive(Debug)]
struct ReauthenticatingSessionState {
    session_id: String,
    identity: ClientIdentity,
    _enhanced_auth_method: String,
    protocol_level: u8,
    receive_maximum: usize,
    include_problem_information: bool,
    client_max_packet_size: Option<u32>,
    current_session_expiry_interval: Option<u32>,
    will_delay_interval_secs: Option<u32>,
    publish_will: bool,
    topic_aliases: HashMap<u16, String>,
}

#[derive(Debug)]
struct DisconnectingSessionState {
    session_id: String,
    protocol_level: u8,
    receive_maximum: usize,
    include_problem_information: bool,
    client_max_packet_size: Option<u32>,
    current_session_expiry_interval: Option<u32>,
    will_delay_interval_secs: Option<u32>,
    publish_will: bool,
    topic_aliases: HashMap<u16, String>,
    session_expiry_interval: Option<u32>,
}

#[derive(Debug)]
pub(super) struct ProtocolSessionState {
    phase: SessionPhase,
}

#[derive(Debug)]
enum SessionPhase {
    AwaitConnect,
    Established(EstablishedSessionState),
    ReAuthenticating(ReauthenticatingSessionState),
    Disconnecting(DisconnectingSessionState),
}

impl Default for ProtocolSessionState {
    fn default() -> Self {
        Self {
            phase: SessionPhase::AwaitConnect,
        }
    }
}

impl ProtocolSessionState {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_connected(
        &mut self,
        session_id: String,
        identity: ClientIdentity,
        enhanced_auth_method: Option<String>,
        protocol_level: u8,
        receive_maximum: usize,
        include_problem_information: bool,
        client_max_packet_size: Option<u32>,
    ) {
        self.on_connected_with_lifecycle(
            session_id,
            identity,
            enhanced_auth_method,
            protocol_level,
            receive_maximum,
            include_problem_information,
            client_max_packet_size,
            None,
            None,
            true,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_connected_with_lifecycle(
        &mut self,
        session_id: String,
        identity: ClientIdentity,
        enhanced_auth_method: Option<String>,
        protocol_level: u8,
        receive_maximum: usize,
        include_problem_information: bool,
        client_max_packet_size: Option<u32>,
        current_session_expiry_interval: Option<u32>,
        will_delay_interval_secs: Option<u32>,
        publish_will: bool,
    ) {
        self.phase = SessionPhase::Established(EstablishedSessionState {
            session_id,
            identity,
            enhanced_auth_method,
            protocol_level,
            receive_maximum,
            include_problem_information,
            client_max_packet_size,
            current_session_expiry_interval,
            will_delay_interval_secs,
            publish_will,
            topic_aliases: HashMap::new(),
        });
    }

    pub(super) fn begin_disconnect(&mut self, session_expiry_interval: Option<u32>) {
        self.begin_disconnect_with_publish_will(session_expiry_interval, false);
    }

    pub(super) fn begin_disconnect_with_publish_will(
        &mut self,
        session_expiry_interval: Option<u32>,
        publish_will: bool,
    ) {
        self.phase = match std::mem::replace(&mut self.phase, SessionPhase::AwaitConnect) {
            SessionPhase::Established(state) => {
                SessionPhase::Disconnecting(DisconnectingSessionState {
                    session_id: state.session_id,
                    protocol_level: state.protocol_level,
                    receive_maximum: state.receive_maximum,
                    include_problem_information: state.include_problem_information,
                    client_max_packet_size: state.client_max_packet_size,
                    current_session_expiry_interval: state.current_session_expiry_interval,
                    will_delay_interval_secs: state.will_delay_interval_secs,
                    publish_will,
                    topic_aliases: state.topic_aliases,
                    session_expiry_interval,
                })
            }
            SessionPhase::ReAuthenticating(state) => {
                SessionPhase::Disconnecting(DisconnectingSessionState {
                    session_id: state.session_id,
                    protocol_level: state.protocol_level,
                    receive_maximum: state.receive_maximum,
                    include_problem_information: state.include_problem_information,
                    client_max_packet_size: state.client_max_packet_size,
                    current_session_expiry_interval: state.current_session_expiry_interval,
                    will_delay_interval_secs: state.will_delay_interval_secs,
                    publish_will,
                    topic_aliases: state.topic_aliases,
                    session_expiry_interval,
                })
            }
            SessionPhase::Disconnecting(state) => SessionPhase::Disconnecting(state),
            SessionPhase::AwaitConnect => SessionPhase::AwaitConnect,
        };
    }

    pub(super) fn prepare_packet(
        &self,
        kind: ProtocolPacketKind,
    ) -> Result<(), ProtocolStateError> {
        match kind {
            ProtocolPacketKind::Connect => match self.phase {
                SessionPhase::AwaitConnect => Ok(()),
                SessionPhase::Established(_)
                | SessionPhase::ReAuthenticating(_)
                | SessionPhase::Disconnecting(_) => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: "unexpected connect packet",
                }),
            },
            _ => match self.phase {
                SessionPhase::Established(_) => Ok(()),
                SessionPhase::AwaitConnect => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: kind.before_connect_reason(),
                }),
                SessionPhase::ReAuthenticating(_) => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: "packet during re-authentication",
                }),
                SessionPhase::Disconnecting(_) => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: "packet during disconnecting",
                }),
            },
        }
    }

    pub(super) fn active_session_id(&self) -> Option<&String> {
        match &self.phase {
            SessionPhase::Established(state) => Some(&state.session_id),
            SessionPhase::ReAuthenticating(state) => Some(&state.session_id),
            SessionPhase::Disconnecting(state) => Some(&state.session_id),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn has_active_session(&self) -> bool {
        self.active_session_id().is_some()
    }

    pub(super) fn receive_maximum(&self) -> Option<usize> {
        match &self.phase {
            SessionPhase::Established(state) => Some(state.receive_maximum),
            SessionPhase::ReAuthenticating(state) => Some(state.receive_maximum),
            SessionPhase::Disconnecting(state) => Some(state.receive_maximum),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn protocol_level(&self) -> Option<u8> {
        match &self.phase {
            SessionPhase::Established(state) => Some(state.protocol_level),
            SessionPhase::ReAuthenticating(state) => Some(state.protocol_level),
            SessionPhase::Disconnecting(state) => Some(state.protocol_level),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn include_problem_information(&self) -> Option<bool> {
        match &self.phase {
            SessionPhase::Established(state) => Some(state.include_problem_information),
            SessionPhase::ReAuthenticating(state) => Some(state.include_problem_information),
            SessionPhase::Disconnecting(state) => Some(state.include_problem_information),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn client_max_packet_size(&self) -> Option<u32> {
        match &self.phase {
            SessionPhase::Established(state) => state.client_max_packet_size,
            SessionPhase::ReAuthenticating(state) => state.client_max_packet_size,
            SessionPhase::Disconnecting(state) => state.client_max_packet_size,
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn current_session_expiry_interval(&self) -> Option<u32> {
        match &self.phase {
            SessionPhase::Established(state) => state.current_session_expiry_interval,
            SessionPhase::ReAuthenticating(state) => state.current_session_expiry_interval,
            SessionPhase::Disconnecting(state) => state.current_session_expiry_interval,
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn will_delay_interval_secs(&self) -> Option<u32> {
        match &self.phase {
            SessionPhase::Established(state) => state.will_delay_interval_secs,
            SessionPhase::ReAuthenticating(state) => state.will_delay_interval_secs,
            SessionPhase::Disconnecting(state) => state.will_delay_interval_secs,
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn publish_will(&self) -> Option<bool> {
        match &self.phase {
            SessionPhase::Established(state) => Some(state.publish_will),
            SessionPhase::ReAuthenticating(state) => Some(state.publish_will),
            SessionPhase::Disconnecting(state) => Some(state.publish_will),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn effective_session_expiry_interval(&self) -> Option<u32> {
        self.disconnect_session_expiry_interval()
            .or(self.current_session_expiry_interval())
    }

    pub(super) fn effective_will_delay_interval_secs(&self) -> Option<u32> {
        let will_delay = self.will_delay_interval_secs()?;
        let effective_session_expiry = self.effective_session_expiry_interval();
        if self.publish_will() == Some(false) {
            return None;
        }
        Some(
            effective_session_expiry
                .map(|expiry| will_delay.min(expiry))
                .unwrap_or(will_delay),
        )
    }

    pub(super) fn resolve_publish_topic(
        &mut self,
        topic_alias: Option<u16>,
        topic: &str,
    ) -> anyhow::Result<String> {
        let alias_map = match &mut self.phase {
            SessionPhase::Established(state) => &mut state.topic_aliases,
            SessionPhase::ReAuthenticating(state) => &mut state.topic_aliases,
            SessionPhase::Disconnecting(state) => &mut state.topic_aliases,
            SessionPhase::AwaitConnect => {
                return match (topic_alias, topic.is_empty()) {
                    (Some(0), _) => anyhow::bail!("invalid topic alias 0"),
                    (Some(_), true) => anyhow::bail!("unknown topic alias before connect"),
                    (Some(_), false) => Ok(topic.to_string()),
                    (None, true) => anyhow::bail!("empty publish topic without topic alias"),
                    (None, false) => Ok(topic.to_string()),
                };
            }
        };
        match (topic_alias, topic.is_empty()) {
            (Some(0), _) => anyhow::bail!("invalid topic alias 0"),
            (Some(alias), false) => {
                alias_map.insert(alias, topic.to_string());
                Ok(topic.to_string())
            }
            (Some(alias), true) => alias_map
                .get(&alias)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("unknown topic alias {alias}")),
            (None, false) => Ok(topic.to_string()),
            (None, true) => anyhow::bail!("empty publish topic without topic alias"),
        }
    }

    pub(super) fn active_session_id_cloned(&self) -> Option<String> {
        self.active_session_id().cloned()
    }

    #[cfg(test)]
    pub(super) fn is_disconnecting(&self) -> bool {
        matches!(self.phase, SessionPhase::Disconnecting(_))
    }

    pub(super) fn disconnect_session_expiry_interval(&self) -> Option<u32> {
        match &self.phase {
            SessionPhase::Disconnecting(state) => state.session_expiry_interval,
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn disconnect_context(&self) -> Option<DisconnectSessionContext> {
        match &self.phase {
            SessionPhase::Disconnecting(state) => Some(DisconnectSessionContext {
                session_id: state.session_id.clone(),
                current_session_expiry_interval: state.current_session_expiry_interval,
                disconnect_session_expiry_interval: state.session_expiry_interval,
                will_delay_interval_secs: state.will_delay_interval_secs,
                publish_will: state.publish_will,
            }),
            _ => None,
        }
    }

    pub(super) fn require_established_session(
        &self,
        reason_string: &'static str,
    ) -> Result<&String, ProtocolStateError> {
        let SessionPhase::Established(state) = &self.phase else {
            return match &self.phase {
                SessionPhase::ReAuthenticating(_) => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: "packet during re-authentication",
                }),
                SessionPhase::Disconnecting(_) => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string: "packet during disconnecting",
                }),
                SessionPhase::AwaitConnect => Err(ProtocolStateError {
                    reason_code: 0x82,
                    reason_string,
                }),
                SessionPhase::Established(_) => unreachable!(),
            };
        };
        Ok(&state.session_id)
    }

    pub(super) fn session_id_for_packet(
        &self,
        kind: ProtocolPacketKind,
    ) -> Result<String, ProtocolStateError> {
        self.require_established_session(kind.before_connect_reason())
            .cloned()
    }

    pub(super) fn take_session_id(&mut self) -> Option<String> {
        match std::mem::replace(&mut self.phase, SessionPhase::AwaitConnect) {
            SessionPhase::Established(state) => Some(state.session_id),
            SessionPhase::ReAuthenticating(state) => Some(state.session_id),
            SessionPhase::Disconnecting(state) => Some(state.session_id),
            SessionPhase::AwaitConnect => None,
        }
    }

    pub(super) fn prepare_session_reauth(
        &self,
        auth: &AuthPacket,
    ) -> Result<SessionReauthRequest, ProtocolStateError> {
        let SessionPhase::Established(state) = &self.phase else {
            return Err(ProtocolStateError {
                reason_code: 0x82,
                reason_string: "unexpected auth packet",
            });
        };
        let auth_method = if let Some(current_method) = state.enhanced_auth_method.as_deref() {
            if let Some(method) = auth.auth_method.as_deref() {
                if method != current_method {
                    return Err(ProtocolStateError {
                        reason_code: 0x8C,
                        reason_string: "unsupported authentication method",
                    });
                }
            }
            current_method.to_string()
        } else if auth.reason_code == 0x00 {
            return Err(ProtocolStateError {
                reason_code: 0x82,
                reason_string: "invalid auth reason code",
            });
        } else if let Some(method) = auth.auth_method.as_deref() {
            method.to_string()
        } else {
            return Err(ProtocolStateError {
                reason_code: 0x82,
                reason_string: "unexpected auth packet",
            });
        };
        Ok(SessionReauthRequest {
            identity: state.identity.clone(),
            auth_method,
        })
    }

    pub(super) fn start_session_reauth(
        &mut self,
        auth: &AuthPacket,
    ) -> Result<SessionReauthRequest, ProtocolStateError> {
        let request = self.prepare_session_reauth(auth)?;
        let SessionPhase::Established(state) = &self.phase else {
            return Err(ProtocolStateError {
                reason_code: 0x82,
                reason_string: "unexpected auth packet",
            });
        };
        self.phase = SessionPhase::ReAuthenticating(ReauthenticatingSessionState {
            session_id: state.session_id.clone(),
            identity: state.identity.clone(),
            _enhanced_auth_method: request.auth_method.clone(),
            protocol_level: state.protocol_level,
            receive_maximum: state.receive_maximum,
            include_problem_information: state.include_problem_information,
            client_max_packet_size: state.client_max_packet_size,
            current_session_expiry_interval: state.current_session_expiry_interval,
            will_delay_interval_secs: state.will_delay_interval_secs,
            publish_will: state.publish_will,
            topic_aliases: state.topic_aliases.clone(),
        });
        Ok(request)
    }

    pub(super) fn complete_session_reauth(&mut self, auth_method: String) {
        match &mut self.phase {
            SessionPhase::Established(state) => {
                state.enhanced_auth_method = Some(auth_method);
            }
            SessionPhase::ReAuthenticating(state) => {
                let next = EstablishedSessionState {
                    session_id: state.session_id.clone(),
                    identity: state.identity.clone(),
                    enhanced_auth_method: Some(auth_method),
                    protocol_level: state.protocol_level,
                    receive_maximum: state.receive_maximum,
                    include_problem_information: state.include_problem_information,
                    client_max_packet_size: state.client_max_packet_size,
                    current_session_expiry_interval: state.current_session_expiry_interval,
                    will_delay_interval_secs: state.will_delay_interval_secs,
                    publish_will: state.publish_will,
                    topic_aliases: state.topic_aliases.clone(),
                };
                self.phase = SessionPhase::Established(next);
            }
            SessionPhase::Disconnecting(_) => {}
            SessionPhase::AwaitConnect => {}
        }
    }
}

impl ProtocolPacketKind {
    fn before_connect_reason(self) -> &'static str {
        match self {
            ProtocolPacketKind::Connect => "unexpected connect packet",
            ProtocolPacketKind::Subscribe => "subscribe before connect",
            ProtocolPacketKind::Unsubscribe => "unsubscribe before connect",
            ProtocolPacketKind::Publish => "publish before connect",
            ProtocolPacketKind::PubAck => "puback before connect",
            ProtocolPacketKind::PubRec => "pubrec before connect",
            ProtocolPacketKind::PubRel => "pubrel before connect",
            ProtocolPacketKind::PubComp => "pubcomp before connect",
            ProtocolPacketKind::PingReq => "pingreq before connect",
            ProtocolPacketKind::Disconnect => "disconnect before connect",
        }
    }
}
