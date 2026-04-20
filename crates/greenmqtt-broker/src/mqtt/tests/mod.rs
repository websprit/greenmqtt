pub(super) use crate::mqtt::codec::{
    parse_packet_frame, read_varint_from_frame, PACKET_TYPE_SUBACK, PACKET_TYPE_UNSUBACK,
};
pub(super) use crate::mqtt::connect::build_disconnect_packet_with_server_reference;
pub(super) use crate::mqtt::transport::{
    ensure_rustls_provider_installed, serve_quic, serve_tcp, serve_tls, serve_ws, serve_wss,
};
pub(super) use crate::mqtt::{
    encode_remaining_length, property_section_end, read_binary, read_u16, read_u32, read_u8,
    read_utf8, read_varint_from_slice, Packet, PACKET_TYPE_AUTH, PACKET_TYPE_CONNACK,
    PACKET_TYPE_DISCONNECT, PACKET_TYPE_PINGRESP, PACKET_TYPE_PUBACK, PACKET_TYPE_PUBCOMP,
    PACKET_TYPE_PUBLISH, PACKET_TYPE_PUBREC, PACKET_TYPE_PUBREL,
};
pub(super) use crate::{BrokerConfig, BrokerRuntime, DefaultBroker};
pub(super) use futures_util::{SinkExt, StreamExt};
pub(super) use greenmqtt_core::{
    ClientIdentity, ConnectRequest, InflightMessage, PublishProperties, SessionKind,
    SessionRecord, UserProperty,
};
pub(super) use greenmqtt_dist::{DistHandle, PersistentDistHandle};
pub(super) use greenmqtt_inbox::PersistentInboxHandle;
pub(super) use greenmqtt_plugin_api::{
    AclAction, AclDecision, AclRule, AllowAllAcl, AllowAllAuth, AuthProvider, ConfiguredAcl,
    ConfiguredAuth, EnhancedAuthResult, HttpAuthConfig, HttpAuthProvider, IdentityMatcher,
    NoopEventHook, StaticAclProvider, StaticAuthProvider, StaticEnhancedAuthProvider,
};
pub(super) use greenmqtt_retain::RetainHandle;
pub(super) use greenmqtt_sessiondict::{PersistentSessionDictHandle, SessionDictHandle};
pub(super) use greenmqtt_storage::{
    InflightStore, MemoryInboxStore, MemoryInflightStore, MemorySubscriptionStore,
};
pub(super) use quinn::{ClientConfig as QuinnClientConfig, Endpoint as QuinnEndpoint};
pub(super) use rcgen::generate_simple_self_signed;
pub(super) use std::net::SocketAddr;
pub(super) use std::path::PathBuf;
pub(super) use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Mutex, OnceLock};
pub(super) use tempfile::TempDir;
pub(super) use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
pub(super) use tokio::net::{TcpListener as TokioTcpListener, TcpStream as TokioTcpStream};
pub(super) use tokio::time::{sleep, timeout, Duration};
pub(super) use tokio_rustls::rustls::pki_types::ServerName;
pub(super) use tokio_rustls::rustls::{
    ClientConfig as RustlsClientConfig, RootCertStore as RustlsRootCertStore,
};
pub(super) use tokio_rustls::TlsConnector;
pub(super) use tokio_tungstenite::{
    client_async, connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

pub(super) fn test_broker() -> Arc<DefaultBroker> {
    test_broker_with_config(BrokerConfig {
        node_id: 1,
        enable_tcp: true,
        enable_tls: false,
        enable_ws: false,
        enable_wss: false,
        enable_quic: false,
        server_keep_alive_secs: None,
        max_packet_size: None,
        response_information: None,
        server_reference: None,
        audit_log_path: None,
    })
}

static TEST_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) struct ScopedEnvVars {
    previous: Vec<(String, Option<String>)>,
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl Drop for ScopedEnvVars {
    fn drop(&mut self) {
        for (key, value) in self.previous.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

pub(super) fn scoped_env_vars(vars: &[(&str, Option<&str>)]) -> ScopedEnvVars {
    let guard = TEST_ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("test env lock poisoned");
    let previous = vars
        .iter()
        .map(|(key, value)| {
            let previous = std::env::var(key).ok();
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
            ((*key).to_string(), previous)
        })
        .collect();
    ScopedEnvVars {
        previous,
        _guard: guard,
    }
}

pub(super) fn next_test_bind() -> SocketAddr {
    static NEXT_TEST_PORT: AtomicU16 = AtomicU16::new(20000);
    SocketAddr::from((
        [127, 0, 0, 1],
        NEXT_TEST_PORT.fetch_add(1, Ordering::Relaxed),
    ))
}

pub(super) async fn connect_quic_with_retry(
    endpoint: &QuinnEndpoint,
    bind: SocketAddr,
) -> quinn::Connection {
    let mut last_error = None;
    for _ in 0..10 {
        match endpoint.connect(bind, "localhost").unwrap().await {
            Ok(connection) => return connection,
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("quic connect timed out: {:?}", last_error);
}

pub(super) struct TcpStream;

impl TcpStream {
    async fn connect(bind: SocketAddr) -> std::io::Result<TokioTcpStream> {
        connect_tcp_with_retry(bind).await
    }
}

pub(super) async fn connect_tcp_with_retry(bind: SocketAddr) -> std::io::Result<TokioTcpStream> {
    let mut last_error = None;
    for _ in 0..160 {
        match TokioTcpStream::connect(bind).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(30)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "tcp connect timed out")
    }))
}

pub(super) async fn read_http_request_for_test(
    stream: &mut TokioTcpStream,
) -> anyhow::Result<String> {
    let mut buffer = Vec::new();
    let mut header_end = None;
    while header_end.is_none() {
        let mut chunk = [0u8; 256];
        let read = stream.read(&mut chunk).await?;
        anyhow::ensure!(read > 0, "unexpected eof while reading request headers");
        buffer.extend_from_slice(&chunk[..read]);
        header_end = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    }
    let header_end = header_end.unwrap() + 4;
    let header_text = String::from_utf8(buffer[..header_end].to_vec())?;
    let content_length = header_text
        .lines()
        .find_map(|line| {
            line.strip_prefix("Content-Length: ")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let mut chunk = [0u8; 256];
        let read = stream.read(&mut chunk).await?;
        anyhow::ensure!(read > 0, "unexpected eof while reading request body");
        buffer.extend_from_slice(&chunk[..read]);
    }
    Ok(String::from_utf8(
        buffer[..header_end + content_length].to_vec(),
    )?)
}

pub(super) type TestWsStream = WebSocketStream<MaybeTlsStream<TokioTcpStream>>;

pub(super) async fn connect_ws_with_retry(url: &str) -> anyhow::Result<TestWsStream> {
    let mut last_error = None;
    for _ in 0..80 {
        match connect_async(url).await {
            Ok((stream, _)) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(30)).await;
            }
        }
    }
    Err(anyhow::anyhow!("ws connect timed out: {:?}", last_error))
}

pub(super) fn test_ws_broker() -> Arc<BrokerRuntime<AllowAllAuth, AllowAllAcl, NoopEventHook>> {
    test_broker_with_custom_auth(
        BrokerConfig {
            node_id: 1,
            enable_tcp: false,
            enable_tls: false,
            enable_ws: true,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
    )
}

pub(super) async fn assert_ws_connection_closed(client: &mut TestWsStream) {
    let next = timeout(Duration::from_secs(1), client.next())
        .await
        .expect("expected websocket session to close");
    match next {
        None | Some(Ok(Message::Close(_))) => {}
        Some(Err(_)) => {}
        Some(Ok(message)) => panic!("expected websocket to close, got {message:?}"),
    }
}

pub(super) fn test_broker_with_config(config: BrokerConfig) -> Arc<DefaultBroker> {
    let inbox = PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    );
    Arc::new(BrokerRuntime::with_plugins(
        config,
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(inbox),
        Arc::new(RetainHandle::default()),
    ))
}

pub(super) fn test_broker_with_inflight_store(
    config: BrokerConfig,
    inflight_store: Arc<dyn InflightStore>,
) -> Arc<DefaultBroker> {
    let inbox = PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        inflight_store,
    );
    Arc::new(BrokerRuntime::with_plugins(
        config,
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(inbox),
        Arc::new(RetainHandle::default()),
    ))
}

pub(super) fn test_broker_with_auth(
    config: BrokerConfig,
    auth: ConfiguredAuth,
) -> Arc<BrokerRuntime<ConfiguredAuth, AllowAllAcl, NoopEventHook>> {
    test_broker_with_custom_auth(config, auth)
}

pub(super) fn test_broker_with_custom_auth<A: AuthProvider + Clone + 'static>(
    config: BrokerConfig,
    auth: A,
) -> Arc<BrokerRuntime<A, AllowAllAcl, NoopEventHook>> {
    let inbox = PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    );
    Arc::new(BrokerRuntime::with_plugins(
        config,
        auth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(inbox),
        Arc::new(RetainHandle::default()),
    ))
}

#[derive(Clone)]
pub(super) struct AllowThenStaticEnhancedAuthProvider {
    inner: StaticEnhancedAuthProvider,
}

impl AllowThenStaticEnhancedAuthProvider {
    fn new(inner: StaticEnhancedAuthProvider) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl AuthProvider for AllowThenStaticEnhancedAuthProvider {
    async fn authenticate(
        &self,
        _identity: &greenmqtt_core::ClientIdentity,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn begin_enhanced_auth(
        &self,
        identity: &greenmqtt_core::ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        self.inner
            .begin_enhanced_auth(identity, method, auth_data)
            .await
    }

    async fn continue_enhanced_auth(
        &self,
        identity: &greenmqtt_core::ClientIdentity,
        method: &str,
        auth_data: Option<&[u8]>,
    ) -> anyhow::Result<EnhancedAuthResult> {
        self.inner
            .continue_enhanced_auth(identity, method, auth_data)
            .await
    }
}

#[derive(Default)]
pub(super) struct CountingReplayInflightStore {
    inner: MemoryInflightStore,
    save_inflight_calls: AtomicUsize,
    save_inflight_batch_calls: AtomicUsize,
}

impl CountingReplayInflightStore {
    fn save_inflight_calls(&self) -> usize {
        self.save_inflight_calls.load(Ordering::SeqCst)
    }

    fn save_inflight_batch_calls(&self) -> usize {
        self.save_inflight_batch_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl InflightStore for CountingReplayInflightStore {
    async fn save_inflight(&self, message: &InflightMessage) -> anyhow::Result<()> {
        self.save_inflight_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.save_inflight(message).await
    }

    async fn save_inflight_batch(&self, messages: &[InflightMessage]) -> anyhow::Result<()> {
        self.save_inflight_batch_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.save_inflight_batch(messages).await
    }

    async fn load_inflight(&self, session_id: &str) -> anyhow::Result<Vec<InflightMessage>> {
        self.inner.load_inflight(session_id).await
    }

    async fn list_all_inflight(&self) -> anyhow::Result<Vec<InflightMessage>> {
        self.inner.list_all_inflight().await
    }

    async fn delete_inflight(&self, session_id: &str, packet_id: u16) -> anyhow::Result<()> {
        self.inner.delete_inflight(session_id, packet_id).await
    }

    async fn count_inflight(&self) -> anyhow::Result<usize> {
        self.inner.count_inflight().await
    }
}

pub(super) fn test_broker_with_acl(
    config: BrokerConfig,
    acl: ConfiguredAcl,
) -> Arc<BrokerRuntime<AllowAllAuth, ConfiguredAcl, NoopEventHook>> {
    let inbox = PersistentInboxHandle::open(
        Arc::new(MemorySubscriptionStore::default()),
        Arc::new(MemoryInboxStore::default()),
        Arc::new(MemoryInflightStore::default()),
    );
    Arc::new(BrokerRuntime::with_plugins(
        config,
        AllowAllAuth,
        acl,
        NoopEventHook,
        Arc::new(SessionDictHandle::default()),
        Arc::new(DistHandle::default()),
        Arc::new(inbox),
        Arc::new(RetainHandle::default()),
    ))
}

pub(super) async fn persistent_test_broker(data_dir: &TempDir) -> Arc<DefaultBroker> {
    let session_store = Arc::new(
        greenmqtt_storage::SledSessionStore::open(data_dir.path().join("sessions")).unwrap(),
    );
    let route_store =
        Arc::new(greenmqtt_storage::SledRouteStore::open(data_dir.path().join("routes")).unwrap());
    let subscription_store = Arc::new(
        greenmqtt_storage::SledSubscriptionStore::open(data_dir.path().join("subscriptions"))
            .unwrap(),
    );
    let inbox_store =
        Arc::new(greenmqtt_storage::SledInboxStore::open(data_dir.path().join("inbox")).unwrap());
    let inflight_store = Arc::new(
        greenmqtt_storage::SledInflightStore::open(data_dir.path().join("inflight")).unwrap(),
    );
    Arc::new(BrokerRuntime::with_plugins(
        BrokerConfig {
            node_id: 1,
            enable_tcp: true,
            enable_tls: false,
            enable_ws: false,
            enable_wss: false,
            enable_quic: false,
            server_keep_alive_secs: None,
            max_packet_size: None,
            response_information: None,
            server_reference: None,
            audit_log_path: None,
        },
        AllowAllAuth,
        AllowAllAcl,
        NoopEventHook,
        Arc::new(
            PersistentSessionDictHandle::open(session_store)
                .await
                .unwrap(),
        ),
        Arc::new(PersistentDistHandle::open(route_store).await.unwrap()),
        Arc::new(PersistentInboxHandle::open(
            subscription_store,
            inbox_store,
            inflight_store,
        )),
        Arc::new(RetainHandle::default()),
    ))
}

pub(super) fn write_self_signed_tls_material() -> (
    tempfile::TempDir,
    PathBuf,
    PathBuf,
    rcgen::CertifiedKey<rcgen::KeyPair>,
) {
    ensure_rustls_provider_installed();
    let cert = generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();
    let tempdir = tempfile::tempdir().unwrap();
    let cert_path = tempdir.path().join("server.crt");
    let key_path = tempdir.path().join("server.key");
    std::fs::write(&cert_path, cert_pem.as_bytes()).unwrap();
    std::fs::write(&key_path, key_pem.as_bytes()).unwrap();
    (tempdir, cert_path, key_path, cert)
}

pub(super) fn connect_packet(client_id: &str) -> Vec<u8> {
    connect_packet_with_username_and_clean_start(client_id, None, None, true)
}

pub(super) fn connect_packet_v5(client_id: &str) -> Vec<u8> {
    connect_packet_v5_with_properties(client_id, true, &[])
}

pub(super) fn connect_packet_v5_with_keep_alive(
    client_id: &str,
    clean_start: bool,
    keep_alive_secs: u16,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(5);
    body.push(if clean_start { 0b0000_0010 } else { 0 });
    body.extend_from_slice(&keep_alive_secs.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    let mut packet = vec![0x10];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn connect_packet_v5_with_properties(
    client_id: &str,
    clean_start: bool,
    properties: &[u8],
) -> Vec<u8> {
    connect_packet_v5_with_keep_alive(client_id, clean_start, 30, properties)
}

pub(super) fn connect_packet_v5_with_will(
    client_id: &str,
    will_topic: &str,
    will_payload: &[u8],
    will_qos: u8,
    will_retain: bool,
    will_properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(5);
    let mut flags = 0b0000_0010 | 0b0000_0100;
    flags |= (will_qos & 0b11) << 3;
    if will_retain {
        flags |= 0b0010_0000;
    }
    body.push(flags);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    encode_remaining_length(&mut body, will_properties.len());
    body.extend_from_slice(will_properties);
    body.extend_from_slice(&(will_topic.len() as u16).to_be_bytes());
    body.extend_from_slice(will_topic.as_bytes());
    body.extend_from_slice(&(will_payload.len() as u16).to_be_bytes());
    body.extend_from_slice(will_payload);
    let mut packet = vec![0x10];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn connect_packet_with_clean_start(client_id: &str, clean_start: bool) -> Vec<u8> {
    connect_packet_with_username_and_clean_start(client_id, None, None, clean_start)
}

pub(super) fn connect_packet_with_will(
    client_id: &str,
    will_topic: &str,
    will_payload: &[u8],
    will_qos: u8,
    will_retain: bool,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(4);
    let mut flags = 0b0000_0010 | 0b0000_0100;
    flags |= (will_qos & 0b11) << 3;
    if will_retain {
        flags |= 0b0010_0000;
    }
    body.push(flags);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(will_topic.len() as u16).to_be_bytes());
    body.extend_from_slice(will_topic.as_bytes());
    body.extend_from_slice(&(will_payload.len() as u16).to_be_bytes());
    body.extend_from_slice(will_payload);
    let mut packet = vec![0x10];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn connect_packet_with_username(
    client_id: &str,
    username: Option<&str>,
    password: Option<&[u8]>,
) -> Vec<u8> {
    connect_packet_with_username_and_clean_start(client_id, username, password, true)
}

pub(super) fn connect_packet_with_username_and_clean_start(
    client_id: &str,
    username: Option<&str>,
    password: Option<&[u8]>,
    clean_start: bool,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(4);
    let mut flags = if clean_start { 0b0000_0010 } else { 0 };
    if username.is_some() {
        flags |= 0b1000_0000;
    }
    if password.is_some() {
        flags |= 0b0100_0000;
    }
    body.push(flags);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    if let Some(username) = username {
        body.extend_from_slice(&(username.len() as u16).to_be_bytes());
        body.extend_from_slice(username.as_bytes());
    }
    if let Some(password) = password {
        body.extend_from_slice(&(password.len() as u16).to_be_bytes());
        body.extend_from_slice(password);
    }
    let mut packet = vec![0x10];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn subscribe_packet(packet_id: u16, topic_filter: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_filter.as_bytes());
    body.push(1);
    let mut packet = vec![0x82];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn subscribe_packet_v5(packet_id: u16, topic_filter: &str) -> Vec<u8> {
    subscribe_packet_v5_with_options(packet_id, topic_filter, 1, &[])
}

pub(super) fn subscribe_packet_v5_with_properties(
    packet_id: u16,
    topic_filter: &str,
    properties: &[u8],
) -> Vec<u8> {
    subscribe_packet_v5_with_options(packet_id, topic_filter, 1, properties)
}

pub(super) fn subscribe_packet_v5_with_options(
    packet_id: u16,
    topic_filter: &str,
    options: u8,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_filter.as_bytes());
    body.push(options);
    let mut packet = vec![0x82];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn empty_subscribe_packet_v5(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    let mut packet = vec![0x82];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn disconnect_packet() -> Vec<u8> {
    vec![0xE0, 0x00]
}

pub(super) fn pingreq_packet() -> Vec<u8> {
    vec![0xC0, 0x00]
}

pub(super) fn malformed_remaining_length_packet() -> Vec<u8> {
    vec![0xC0, 0xFF, 0xFF, 0xFF, 0xFF]
}

pub(super) fn pingreq_packet_with_flags(flags: u8) -> Vec<u8> {
    vec![0xC0 | (flags & 0x0F), 0x00]
}

pub(super) fn disconnect_packet_v5_with_flags(flags: u8) -> Vec<u8> {
    vec![0xE0 | (flags & 0x0F), 0x00]
}

pub(super) fn disconnect_packet_v5_with_properties(properties: &[u8]) -> Vec<u8> {
    disconnect_packet_v5_with_reason_and_properties(0x00, properties)
}

pub(super) fn disconnect_packet_v5_with_reason_and_properties(
    reason_code: u8,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = vec![reason_code];
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0xE0];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn unsubscribe_packet(packet_id: u16, topic_filter: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_filter.as_bytes());
    let mut packet = vec![0xA2];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn unsubscribe_packet_v5_with_properties(
    packet_id: u16,
    topic_filter: &str,
    properties: &[u8],
) -> Vec<u8> {
    unsubscribe_packet_v5_multiple_with_properties(packet_id, &[topic_filter], properties)
}

pub(super) fn unsubscribe_packet_v5_multiple_with_properties(
    packet_id: u16,
    topic_filters: &[&str],
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    for topic_filter in topic_filters {
        body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_filter.as_bytes());
    }
    let mut packet = vec![0xA2];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn empty_unsubscribe_packet_v5(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    let mut packet = vec![0xA2];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn publish_packet(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    let mut packet = vec![0x30];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn publish_packet_with_flags(flags: u8, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    let mut packet = vec![(PACKET_TYPE_PUBLISH << 4) | flags];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn publish_packet_v5_retain(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    let mut packet = vec![0x31];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn publish_packet_v5_qos1(packet_id: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    publish_packet_v5_qos1_with_properties(packet_id, topic, payload, &[])
}

pub(super) fn publish_packet_v5_qos2(packet_id: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    let mut packet = vec![0x34];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn publish_packet_v5_qos1_with_properties(
    packet_id: u16,
    topic: &str,
    payload: &[u8],
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    body.extend_from_slice(payload);
    let mut packet = vec![0x32];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn session_expiry_interval_property(value: u32) -> Vec<u8> {
    let mut properties = vec![0x11];
    properties.extend_from_slice(&value.to_be_bytes());
    properties
}

pub(super) fn will_delay_interval_property(value: u32) -> Vec<u8> {
    let mut properties = vec![0x18];
    properties.extend_from_slice(&value.to_be_bytes());
    properties
}

pub(super) fn topic_alias_property(alias: u16) -> Vec<u8> {
    let mut properties = vec![0x23];
    properties.extend_from_slice(&alias.to_be_bytes());
    properties
}

pub(super) fn receive_maximum_property(value: u16) -> Vec<u8> {
    let mut properties = vec![0x21];
    properties.extend_from_slice(&value.to_be_bytes());
    properties
}

pub(super) fn maximum_packet_size_property(value: u32) -> Vec<u8> {
    let mut properties = vec![0x27];
    properties.extend_from_slice(&value.to_be_bytes());
    properties
}

pub(super) fn request_response_information_property(enabled: bool) -> Vec<u8> {
    vec![0x19, u8::from(enabled)]
}

pub(super) fn request_response_information_raw_property(value: u8) -> Vec<u8> {
    vec![0x19, value]
}

pub(super) fn request_problem_information_property(enabled: bool) -> Vec<u8> {
    vec![0x17, u8::from(enabled)]
}

pub(super) fn request_problem_information_raw_property(value: u8) -> Vec<u8> {
    vec![0x17, value]
}

pub(super) fn auth_method_property(method: &str) -> Vec<u8> {
    let mut properties = vec![0x15];
    properties.extend_from_slice(&(method.len() as u16).to_be_bytes());
    properties.extend_from_slice(method.as_bytes());
    properties
}

pub(super) fn auth_data_property(data: &[u8]) -> Vec<u8> {
    let mut properties = vec![0x16];
    properties.extend_from_slice(&(data.len() as u16).to_be_bytes());
    properties.extend_from_slice(data);
    properties
}

pub(super) fn connect_packet_v5_with_auth(
    client_id: &str,
    clean_start: bool,
    auth_method: &str,
    auth_data: Option<&[u8]>,
    extra_properties: &[u8],
) -> Vec<u8> {
    let mut properties = auth_method_property(auth_method);
    if let Some(auth_data) = auth_data {
        properties.extend_from_slice(&auth_data_property(auth_data));
    }
    properties.extend_from_slice(extra_properties);
    connect_packet_v5_with_properties(client_id, clean_start, &properties)
}

pub(super) fn auth_packet_v5_with_reason(
    reason_code: u8,
    method: Option<&str>,
    auth_data: Option<&[u8]>,
) -> Vec<u8> {
    let mut body = vec![reason_code];
    let mut properties = Vec::new();
    if let Some(method) = method {
        properties.extend_from_slice(&auth_method_property(method));
    }
    if let Some(auth_data) = auth_data {
        properties.extend_from_slice(&auth_data_property(auth_data));
    }
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(&properties);
    let mut packet = vec![0xF0];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn auth_packet_v5(method: Option<&str>, auth_data: Option<&[u8]>) -> Vec<u8> {
    auth_packet_v5_with_reason(0x18, method, auth_data)
}

pub(super) fn auth_packet_v5_with_properties(reason_code: u8, properties: &[u8]) -> Vec<u8> {
    let mut body = vec![reason_code];
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0xF0];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) struct ParsedConnackProperties {
    reason_code: u8,
    session_present: bool,
    server_keep_alive_secs: Option<u16>,
    maximum_packet_size: Option<u32>,
    topic_alias_maximum: Option<u16>,
    retain_available: bool,
    wildcard_subscription_available: bool,
    subscription_identifiers_available: bool,
    shared_subscription_available: bool,
    assigned_client_identifier: Option<String>,
    response_information: Option<String>,
    server_reference: Option<String>,
    reason_string: Option<String>,
}

pub(super) struct ParsedAuthPacket {
    reason_code: u8,
    auth_method: Option<String>,
    auth_data: Option<Vec<u8>>,
    reason_string: Option<String>,
}

pub(super) struct ParsedDisconnectProperties {
    reason_code: u8,
    reason_string: Option<String>,
    server_reference: Option<String>,
}

pub(super) fn parse_v5_connack_packet(packet: &[u8]) -> ParsedConnackProperties {
    let mut cursor = 0usize;
    let ack_flags = read_u8(packet, &mut cursor).unwrap();
    let reason_code = read_u8(packet, &mut cursor).unwrap();
    let properties_end = property_section_end(packet, &mut cursor).unwrap();
    let mut parsed = ParsedConnackProperties {
        reason_code,
        session_present: (ack_flags & 0x01) != 0,
        server_keep_alive_secs: None,
        maximum_packet_size: None,
        topic_alias_maximum: None,
        retain_available: false,
        wildcard_subscription_available: false,
        subscription_identifiers_available: false,
        shared_subscription_available: false,
        assigned_client_identifier: None,
        response_information: None,
        server_reference: None,
        reason_string: None,
    };
    while cursor < properties_end {
        match packet[cursor] {
            0x13 => {
                cursor += 1;
                parsed.server_keep_alive_secs = Some(read_u16(packet, &mut cursor).unwrap());
            }
            0x27 => {
                cursor += 1;
                parsed.maximum_packet_size = Some(read_u32(packet, &mut cursor).unwrap());
            }
            0x22 => {
                cursor += 1;
                parsed.topic_alias_maximum = Some(read_u16(packet, &mut cursor).unwrap());
            }
            0x25 => {
                cursor += 1;
                parsed.retain_available = read_u8(packet, &mut cursor).unwrap() != 0;
            }
            0x28 => {
                cursor += 1;
                parsed.wildcard_subscription_available = read_u8(packet, &mut cursor).unwrap() != 0;
            }
            0x29 => {
                cursor += 1;
                parsed.subscription_identifiers_available =
                    read_u8(packet, &mut cursor).unwrap() != 0;
            }
            0x2A => {
                cursor += 1;
                parsed.shared_subscription_available = read_u8(packet, &mut cursor).unwrap() != 0;
            }
            0x12 => {
                cursor += 1;
                parsed.assigned_client_identifier = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x1A => {
                cursor += 1;
                parsed.response_information = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x1C => {
                cursor += 1;
                parsed.server_reference = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x1F => {
                cursor += 1;
                parsed.reason_string = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            other => panic!("unexpected connack property {other:#x}"),
        }
    }
    parsed
}

pub(super) fn parse_v5_auth_packet(packet: &[u8]) -> ParsedAuthPacket {
    let mut cursor = 0usize;
    let reason_code = read_u8(packet, &mut cursor).unwrap();
    let properties_end = property_section_end(packet, &mut cursor).unwrap();
    let mut parsed = ParsedAuthPacket {
        reason_code,
        auth_method: None,
        auth_data: None,
        reason_string: None,
    };
    while cursor < properties_end {
        match packet[cursor] {
            0x15 => {
                cursor += 1;
                parsed.auth_method = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x16 => {
                cursor += 1;
                parsed.auth_data = Some(read_binary(packet, &mut cursor).unwrap());
            }
            0x1F => {
                cursor += 1;
                parsed.reason_string = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            other => panic!("unexpected auth property {other:#x}"),
        }
    }
    parsed
}

pub(super) fn parse_suback_reason_string(packet: &[u8], protocol_level: u8) -> Option<String> {
    if protocol_level != 5 {
        return None;
    }
    let mut cursor = 0usize;
    let _packet_id = read_u16(packet, &mut cursor).unwrap();
    let properties_end = property_section_end(packet, &mut cursor).unwrap();
    if cursor < properties_end {
        match packet[cursor] {
            0x1F => {
                cursor += 1;
                return Some(read_utf8(packet, &mut cursor).unwrap());
            }
            other => panic!("unexpected suback property {other:#x}"),
        }
    }
    None
}

pub(super) fn parse_puback_reason_string(packet: &[u8]) -> Option<String> {
    let mut cursor = 0usize;
    let _packet_id = read_u16(packet, &mut cursor).unwrap();
    let _reason_code = read_u8(packet, &mut cursor).unwrap();
    let properties_end = property_section_end(packet, &mut cursor).unwrap();
    if cursor < properties_end {
        match packet[cursor] {
            0x1F => {
                cursor += 1;
                return Some(read_utf8(packet, &mut cursor).unwrap());
            }
            other => panic!("unexpected puback property {other:#x}"),
        }
    }
    None
}

pub(super) fn parse_v5_disconnect_packet(packet: &[u8]) -> ParsedDisconnectProperties {
    let mut cursor = 0usize;
    let reason_code = read_u8(packet, &mut cursor).unwrap();
    let properties_end = property_section_end(packet, &mut cursor).unwrap();
    let mut parsed = ParsedDisconnectProperties {
        reason_code,
        reason_string: None,
        server_reference: None,
    };
    while cursor < properties_end {
        match packet[cursor] {
            0x1F => {
                cursor += 1;
                parsed.reason_string = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x1C => {
                cursor += 1;
                parsed.server_reference = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            other => panic!("unexpected disconnect property {other:#x}"),
        }
    }
    parsed
}

pub(super) fn parse_suback_return_codes(packet: &[u8], protocol_level: u8) -> Vec<u8> {
    let mut cursor = 0usize;
    let _packet_id = read_u16(packet, &mut cursor).unwrap();
    if protocol_level == 5 {
        cursor = property_section_end(packet, &mut cursor).unwrap();
    }
    packet[cursor..].to_vec()
}

pub(super) fn parse_unsuback_return_codes(packet: &[u8], protocol_level: u8) -> Vec<u8> {
    let mut cursor = 0usize;
    let _packet_id = read_u16(packet, &mut cursor).unwrap();
    if protocol_level == 5 {
        cursor = property_section_end(packet, &mut cursor).unwrap();
    }
    packet[cursor..].to_vec()
}

pub(super) fn parse_puback_reason_code(packet: &[u8]) -> u8 {
    let mut cursor = 0usize;
    let _packet_id = read_u16(packet, &mut cursor).unwrap();
    read_u8(packet, &mut cursor).unwrap()
}

pub(super) fn message_expiry_interval_property(value: u32) -> Vec<u8> {
    let mut properties = vec![0x02];
    properties.extend_from_slice(&value.to_be_bytes());
    properties
}

pub(super) fn subscription_identifier_property(identifier: u32) -> Vec<u8> {
    let mut properties = vec![0x0B];
    encode_remaining_length(&mut properties, identifier as usize);
    properties
}

pub(super) fn user_property(key: &str, value: &str) -> Vec<u8> {
    let mut properties = vec![0x26];
    properties.extend_from_slice(&(key.len() as u16).to_be_bytes());
    properties.extend_from_slice(key.as_bytes());
    properties.extend_from_slice(&(value.len() as u16).to_be_bytes());
    properties.extend_from_slice(value.as_bytes());
    properties
}

pub(super) fn reason_string_property(value: &str) -> Vec<u8> {
    let mut properties = vec![0x1F];
    properties.extend_from_slice(&(value.len() as u16).to_be_bytes());
    properties.extend_from_slice(value.as_bytes());
    properties
}

pub(super) fn topic_alias_and_user_property(alias: u16, key: &str, value: &str) -> Vec<u8> {
    let mut properties = topic_alias_property(alias);
    properties.extend_from_slice(&user_property(key, value));
    properties
}

pub(super) fn publish_properties(
    payload_format_indicator: Option<u8>,
    content_type: Option<&str>,
    message_expiry_interval_secs: Option<u32>,
    response_topic: Option<&str>,
    correlation_data: Option<&[u8]>,
    user_properties: &[(&str, &str)],
) -> Vec<u8> {
    let mut properties = Vec::new();
    if let Some(payload_format_indicator) = payload_format_indicator {
        properties.push(0x01);
        properties.push(payload_format_indicator);
    }
    if let Some(content_type) = content_type {
        properties.push(0x03);
        properties.extend_from_slice(&(content_type.len() as u16).to_be_bytes());
        properties.extend_from_slice(content_type.as_bytes());
    }
    if let Some(expiry_secs) = message_expiry_interval_secs {
        properties.extend_from_slice(&message_expiry_interval_property(expiry_secs));
    }
    if let Some(response_topic) = response_topic {
        properties.push(0x08);
        properties.extend_from_slice(&(response_topic.len() as u16).to_be_bytes());
        properties.extend_from_slice(response_topic.as_bytes());
    }
    if let Some(correlation_data) = correlation_data {
        properties.push(0x09);
        properties.extend_from_slice(&(correlation_data.len() as u16).to_be_bytes());
        properties.extend_from_slice(correlation_data);
    }
    for (key, value) in user_properties {
        properties.extend_from_slice(&user_property(key, value));
    }
    properties
}

pub(super) fn parse_v5_publish_packet(packet: &[u8]) -> (String, PublishProperties, Vec<u8>) {
    let mut cursor = 0usize;
    let topic_len = u16::from_be_bytes([packet[0], packet[1]]) as usize;
    let topic = String::from_utf8(packet[2..2 + topic_len].to_vec()).unwrap();
    cursor += 2 + topic_len;
    cursor += 2;
    let properties_len = read_varint_from_slice(packet, &mut cursor).unwrap() as usize;
    let properties_end = cursor + properties_len;
    let mut payload_format_indicator = None;
    let mut content_type = None;
    let mut message_expiry_interval_secs = None;
    let mut response_topic = None;
    let mut correlation_data = None;
    let mut subscription_identifiers = Vec::new();
    let mut user_properties = Vec::new();
    while cursor < properties_end {
        match packet[cursor] {
            0x01 => {
                cursor += 1;
                payload_format_indicator = Some(packet[cursor]);
                cursor += 1;
            }
            0x03 => {
                cursor += 1;
                content_type = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x02 => {
                cursor += 1;
                message_expiry_interval_secs = Some(read_u32(packet, &mut cursor).unwrap());
            }
            0x08 => {
                cursor += 1;
                response_topic = Some(read_utf8(packet, &mut cursor).unwrap());
            }
            0x09 => {
                cursor += 1;
                correlation_data = Some(read_binary(packet, &mut cursor).unwrap());
            }
            0x0B => {
                cursor += 1;
                subscription_identifiers.push(read_varint_from_slice(packet, &mut cursor).unwrap());
            }
            0x26 => {
                cursor += 1;
                user_properties.push(UserProperty {
                    key: read_utf8(packet, &mut cursor).unwrap(),
                    value: read_utf8(packet, &mut cursor).unwrap(),
                });
            }
            other => panic!("unexpected property {other:#x}"),
        }
    }
    (
        topic,
        PublishProperties {
            payload_format_indicator,
            content_type,
            message_expiry_interval_secs,
            stored_at_ms: None,
            response_topic,
            correlation_data,
            subscription_identifiers,
            user_properties,
        },
        packet[cursor..].to_vec(),
    )
}

pub(super) fn publish_packet_qos2(packet_id: u16, topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(payload);
    let mut packet = vec![0x34];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrel_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    let mut packet = vec![0x62];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrel_packet_v5_with_properties(packet_id: u16, properties: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0x62];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrel_packet_v5_with_reason_code(packet_id: u16, reason_code: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(reason_code);
    body.push(0);
    let mut packet = vec![0x62];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn puback_client_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    let mut packet = vec![0x40];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn puback_client_packet_v5_with_properties(
    packet_id: u16,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0x40];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn puback_client_packet_v5_with_reason_code(packet_id: u16, reason_code: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(reason_code);
    body.push(0);
    let mut packet = vec![0x40];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrec_client_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    let mut packet = vec![0x50];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrec_client_packet_v5_with_properties(
    packet_id: u16,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0x50];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubrec_client_packet_v5_with_reason_code(packet_id: u16, reason_code: u8) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(reason_code);
    body.push(0);
    let mut packet = vec![0x50];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubcomp_client_packet(packet_id: u16) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    let mut packet = vec![0x70];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubcomp_client_packet_v5_with_properties(
    packet_id: u16,
    properties: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    encode_remaining_length(&mut body, properties.len());
    body.extend_from_slice(properties);
    let mut packet = vec![0x70];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn pubcomp_client_packet_v5_with_reason_code(
    packet_id: u16,
    reason_code: u8,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(reason_code);
    body.push(0);
    let mut packet = vec![0x70];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

pub(super) fn parse_outbound_publish_packet_id(packet: &[u8]) -> u16 {
    let topic_len = u16::from_be_bytes([packet[0], packet[1]]) as usize;
    let cursor = 2 + topic_len;
    u16::from_be_bytes([packet[cursor], packet[cursor + 1]])
}

pub(super) async fn read_remaining_length_for_test<S>(stream: &mut S) -> usize
where
    S: AsyncRead + Unpin,
{
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let encoded = stream.read_u8().await.unwrap();
        value += ((encoded & 127) as usize) * multiplier;
        if encoded & 128 == 0 {
            return value;
        }
        multiplier *= 128;
    }
}
pub mod connect;
pub mod fuzz_tests;
pub mod protocol;
pub mod publish;
pub mod quic_tests;
pub mod session;
pub mod subscribe;
pub mod tcp_tests;
pub mod tls_tests;
pub mod ws_tests;
pub mod wss_tests;
