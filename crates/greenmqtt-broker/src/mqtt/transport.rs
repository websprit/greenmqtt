use greenmqtt_plugin_api::{with_listener_profile, AclProvider, AuthProvider, EventHook};
use metrics::counter;
use quinn::{Endpoint, RecvStream, SendStream};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Once;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::handshake::server::{ErrorResponse, Request, Response},
};

use super::session::{drive_session, TcpTransport, WsTransport};
use super::BrokerRuntime;

static RUSTLS_PROVIDER: Once = Once::new();
const PROXY_V1_MAX_HEADER_BYTES: usize = 108;
const PROXY_V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const PROXY_MAX_HEADER_BYTES: usize = 1024;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

struct QuicBiStream {
    send: SendStream,
    recv: RecvStream,
}

impl AsyncRead for QuicBiStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicBiStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut this.send), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut this.send), cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        tokio::io::AsyncWrite::poll_shutdown(Pin::new(&mut this.send), cx)
    }
}

fn proxy_protocol_enabled() -> bool {
    std::env::var("GREENMQTT_PROXY_PROTOCOL")
        .ok()
        .map(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn handshake_timeout() -> Duration {
    std::env::var("GREENMQTT_HANDSHAKE_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT)
}

fn ingress_bandwidth_limits() -> (Option<(u64, u64)>, Option<(u64, u64)>) {
    fn parse_limit(rate_var: &str, burst_var: &str) -> Option<(u64, u64)> {
        let rate = std::env::var(rate_var).ok()?.parse::<u64>().ok()?;
        let burst = std::env::var(burst_var)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(rate.max(1));
        Some((rate.max(1), burst.max(1)))
    }
    (
        parse_limit(
            "GREENMQTT_INGRESS_READ_RATE_PER_SEC",
            "GREENMQTT_INGRESS_READ_BURST",
        ),
        parse_limit(
            "GREENMQTT_INGRESS_WRITE_RATE_PER_SEC",
            "GREENMQTT_INGRESS_WRITE_BURST",
        ),
    )
}

fn websocket_path(env_var: &str) -> String {
    std::env::var(env_var)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/".to_string())
}

fn websocket_request_allowed(request_path: &str, expected_path: &str) -> bool {
    request_path == expected_path
}

fn pre_session_reject_for_pressure<A, C, H>(broker: &BrokerRuntime<A, C, H>) -> bool
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    matches!(
        broker.current_pressure_level(),
        crate::broker::pressure::PressureLevel::Critical
            | crate::broker::pressure::PressureLevel::Emergency
    )
}

fn parse_proxy_protocol_v1_header(buffer: &[u8]) -> anyhow::Result<Option<usize>> {
    if !buffer.starts_with(b"PROXY ") {
        return Ok(None);
    }
    let Some(end) = buffer.windows(2).position(|window| window == b"\r\n") else {
        anyhow::bail!("incomplete proxy protocol header");
    };
    let end = end + 2;
    anyhow::ensure!(
        end <= PROXY_V1_MAX_HEADER_BYTES,
        "proxy protocol header exceeds maximum size"
    );
    let line = std::str::from_utf8(&buffer[..end.saturating_sub(2)])?;
    let parts = line.split_whitespace().collect::<Vec<_>>();
    anyhow::ensure!(parts.len() >= 2, "invalid proxy protocol header");
    anyhow::ensure!(parts[0] == "PROXY", "invalid proxy protocol prefix");
    Ok(Some(end))
}

fn parse_proxy_protocol_v2_header(buffer: &[u8]) -> anyhow::Result<Option<usize>> {
    if !buffer.starts_with(PROXY_V2_SIGNATURE) {
        return Ok(None);
    }
    anyhow::ensure!(buffer.len() >= 16, "incomplete proxy protocol v2 header");
    let version_command = buffer[12];
    anyhow::ensure!(
        (version_command >> 4) == 0x2,
        "invalid proxy protocol v2 version"
    );
    let declared = u16::from_be_bytes([buffer[14], buffer[15]]) as usize;
    let total_len = 16 + declared;
    anyhow::ensure!(
        total_len <= PROXY_MAX_HEADER_BYTES,
        "proxy protocol v2 header exceeds maximum size"
    );
    anyhow::ensure!(
        buffer.len() >= total_len,
        "incomplete proxy protocol v2 header"
    );
    Ok(Some(total_len))
}

async fn maybe_strip_proxy_protocol(stream: &mut tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut peek = vec![0u8; PROXY_MAX_HEADER_BYTES];
    let read = stream.peek(&mut peek).await?;
    if let Some(header_len) = parse_proxy_protocol_v2_header(&peek[..read])? {
        let mut discard = vec![0u8; header_len];
        stream.read_exact(&mut discard).await?;
        return Ok(());
    }
    if let Some(header_len) = parse_proxy_protocol_v1_header(&peek[..read])? {
        let mut discard = vec![0u8; header_len];
        stream.read_exact(&mut discard).await?;
    }
    Ok(())
}

pub async fn serve_tcp<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    serve_tcp_with_profile(broker, bind, "default".to_string()).await
}

pub async fn serve_tcp_with_profile<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    listener_profile: String,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    let proxy_protocol = proxy_protocol_enabled();
    let ingress_limits = ingress_bandwidth_limits();
    loop {
        let (mut stream, _) = listener.accept().await?;
        if pre_session_reject_for_pressure(broker.as_ref()) {
            drop(stream);
            continue;
        }
        if proxy_protocol {
            if let Err(error) = maybe_strip_proxy_protocol(&mut stream).await {
                eprintln!("greenmqtt tcp proxy protocol error: {error:#}");
                continue;
            }
        }
        if !broker.allow_connection_attempt() {
            drop(stream);
            continue;
        }
        let broker = broker.clone();
        let listener_profile = listener_profile.clone();
        let bandwidth = broker.bandwidth_limits();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            if let Err(error) = with_listener_profile(
                listener_profile,
                drive_session(
                    TcpTransport::with_bandwidth(
                        stream,
                        ingress_limits.0.or(bandwidth.0),
                        ingress_limits.1.or(bandwidth.1),
                    ),
                    broker,
                ),
            )
            .await
            {
                eprintln!("greenmqtt tcp session error: {error:#}");
            }
            drop(permit_guard);
        });
    }
}

pub async fn serve_tls<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    serve_tls_with_profile(broker, bind, cert_path, key_path, "default".to_string()).await
}

pub async fn serve_tls_with_profile<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    listener_profile: String,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    let acceptor = load_tls_acceptor(cert_path.as_ref(), key_path.as_ref())?;
    let proxy_protocol = proxy_protocol_enabled();
    let timeout_window = handshake_timeout();
    let ingress_limits = ingress_bandwidth_limits();
    loop {
        let (mut stream, _) = listener.accept().await?;
        if pre_session_reject_for_pressure(broker.as_ref()) {
            drop(stream);
            continue;
        }
        if proxy_protocol {
            if let Err(error) = maybe_strip_proxy_protocol(&mut stream).await {
                eprintln!("greenmqtt tls proxy protocol error: {error:#}");
                continue;
            }
        }
        if !broker.allow_connection_attempt() {
            drop(stream);
            continue;
        }
        let broker = broker.clone();
        let listener_profile = listener_profile.clone();
        let bandwidth = broker.bandwidth_limits();
        let acceptor = acceptor.clone();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            match timeout(timeout_window, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => {
                    if let Err(error) = with_listener_profile(
                        listener_profile,
                        drive_session(
                            TcpTransport::with_bandwidth(
                                stream,
                                ingress_limits.0.or(bandwidth.0),
                                ingress_limits.1.or(bandwidth.1),
                            ),
                            broker,
                        ),
                    )
                    .await
                    {
                        eprintln!("greenmqtt tls session error: {error:#}");
                    }
                }
                Ok(Err(error)) => {
                    eprintln!("greenmqtt tls handshake error: {error:#}");
                }
                Err(_) => {
                    eprintln!("greenmqtt tls handshake timeout");
                }
            }
            drop(permit_guard);
        });
    }
}

pub async fn serve_ws<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    serve_ws_with_profile(broker, bind, "default".to_string()).await
}

pub async fn serve_ws_with_profile<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    listener_profile: String,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    let proxy_protocol = proxy_protocol_enabled();
    let timeout_window = handshake_timeout();
    let ingress_limits = ingress_bandwidth_limits();
    let websocket_path = websocket_path("GREENMQTT_WS_PATH");
    loop {
        let (mut stream, _) = listener.accept().await?;
        if pre_session_reject_for_pressure(broker.as_ref()) {
            drop(stream);
            continue;
        }
        if proxy_protocol {
            if let Err(error) = maybe_strip_proxy_protocol(&mut stream).await {
                eprintln!("greenmqtt ws proxy protocol error: {error:#}");
                continue;
            }
        }
        if !broker.allow_connection_attempt() {
            drop(stream);
            continue;
        }
        let broker = broker.clone();
        let websocket_path = websocket_path.clone();
        let listener_profile = listener_profile.clone();
        let bandwidth = broker.bandwidth_limits();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            let expected_path = websocket_path.clone();
            match timeout(
                timeout_window,
                accept_hdr_async(stream, move |request: &Request, response: Response| {
                    if websocket_request_allowed(request.uri().path(), &expected_path) {
                        Ok(response)
                    } else {
                        let response: ErrorResponse = Response::builder()
                            .status(404)
                            .body(Some(format!("expected websocket path `{expected_path}`")))
                            .expect("valid websocket rejection response");
                        Err(response)
                    }
                }),
            )
            .await
            {
                Ok(stream) => match stream {
                    Ok(stream) => {
                        if let Err(error) = with_listener_profile(
                            listener_profile,
                            drive_session(
                                WsTransport::with_bandwidth(
                                    stream,
                                    ingress_limits.0.or(bandwidth.0),
                                    ingress_limits.1.or(bandwidth.1),
                                ),
                                broker,
                            ),
                        )
                        .await
                        {
                            eprintln!("greenmqtt ws session error: {error:#}");
                        }
                    }
                    Err(error) => {
                        counter!("mqtt_ws_handshake_total", "result" => "error").increment(1);
                        eprintln!("greenmqtt ws handshake error: {error:#}");
                    }
                },
                Err(_) => {
                    counter!("mqtt_ws_handshake_total", "result" => "timeout").increment(1);
                    eprintln!("greenmqtt ws handshake timeout");
                }
            }
            drop(permit_guard);
        });
    }
}

pub async fn serve_wss<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    serve_wss_with_profile(broker, bind, cert_path, key_path, "default".to_string()).await
}

pub async fn serve_wss_with_profile<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    listener_profile: String,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    let acceptor = load_tls_acceptor(cert_path.as_ref(), key_path.as_ref())?;
    let proxy_protocol = proxy_protocol_enabled();
    let timeout_window = handshake_timeout();
    let ingress_limits = ingress_bandwidth_limits();
    let websocket_path = websocket_path("GREENMQTT_WSS_PATH");
    loop {
        let (mut stream, _) = listener.accept().await?;
        if pre_session_reject_for_pressure(broker.as_ref()) {
            drop(stream);
            continue;
        }
        if proxy_protocol {
            if let Err(error) = maybe_strip_proxy_protocol(&mut stream).await {
                eprintln!("greenmqtt wss proxy protocol error: {error:#}");
                continue;
            }
        }
        if !broker.allow_connection_attempt() {
            drop(stream);
            continue;
        }
        let broker = broker.clone();
        let websocket_path = websocket_path.clone();
        let listener_profile = listener_profile.clone();
        let bandwidth = broker.bandwidth_limits();
        let acceptor = acceptor.clone();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            match timeout(timeout_window, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => match timeout(
                    timeout_window,
                    accept_hdr_async(stream, {
                        let expected_path = websocket_path.clone();
                        move |request: &Request, response: Response| {
                            if websocket_request_allowed(request.uri().path(), &expected_path) {
                                Ok(response)
                            } else {
                                let response: ErrorResponse = Response::builder()
                                    .status(404)
                                    .body(Some(format!(
                                        "expected websocket path `{expected_path}`"
                                    )))
                                    .expect("valid websocket rejection response");
                                Err(response)
                            }
                        }
                    }),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        if let Err(error) = with_listener_profile(
                            listener_profile,
                            drive_session(
                                WsTransport::with_bandwidth(
                                    stream,
                                    ingress_limits.0.or(bandwidth.0),
                                    ingress_limits.1.or(bandwidth.1),
                                ),
                                broker,
                            ),
                        )
                        .await
                        {
                            eprintln!("greenmqtt wss session error: {error:#}");
                        }
                    }
                    Ok(Err(error)) => {
                        counter!("mqtt_wss_handshake_total", "result" => "error").increment(1);
                        eprintln!("greenmqtt wss websocket handshake error: {error:#}");
                    }
                    Err(_) => {
                        counter!("mqtt_wss_handshake_total", "result" => "timeout").increment(1);
                        eprintln!("greenmqtt wss websocket handshake timeout");
                    }
                },
                Ok(Err(error)) => {
                    counter!("mqtt_wss_tls_handshake_total", "result" => "error").increment(1);
                    eprintln!("greenmqtt wss tls handshake error: {error:#}");
                }
                Err(_) => {
                    counter!("mqtt_wss_tls_handshake_total", "result" => "timeout").increment(1);
                    eprintln!("greenmqtt wss tls handshake timeout");
                }
            }
            drop(permit_guard);
        });
    }
}

pub async fn serve_quic<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    serve_quic_with_profile(broker, bind, cert_path, key_path, "default".to_string()).await
}

pub async fn serve_quic_with_profile<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    listener_profile: String,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let timeout_window = handshake_timeout();
    let ingress_limits = ingress_bandwidth_limits();
    let endpoint = Endpoint::server(
        load_quic_server_config(cert_path.as_ref(), key_path.as_ref())?,
        bind,
    )?;
    loop {
        let Some(incoming) = endpoint.accept().await else {
            return Ok(());
        };
        let broker = broker.clone();
        let listener_profile = listener_profile.clone();
        let bandwidth = broker.bandwidth_limits();
        tokio::spawn(async move {
            match timeout(timeout_window, incoming).await {
                Ok(connection) => match connection {
                    Ok(connection) => {
                        counter!("mqtt_quic_handshake_total", "result" => "ok").increment(1);
                        if pre_session_reject_for_pressure(broker.as_ref()) {
                            connection.close(0u32.into(), b"server busy");
                            counter!("mqtt_quic_connection_close_total", "reason" => "pressure")
                                .increment(1);
                            return;
                        }
                        if !broker.allow_connection_attempt() {
                            connection.close(0u32.into(), b"connection rate limit exceeded");
                            counter!("mqtt_quic_connection_close_total", "reason" => "rate_limit")
                                .increment(1);
                            return;
                        }
                        let permit = match broker.try_acquire_connection_slot() {
                            Ok(permit) => permit,
                            Err(()) => {
                                connection.close(0u32.into(), b"connection limit exceeded");
                                counter!("mqtt_quic_connection_close_total", "reason" => "connection_limit").increment(1);
                                return;
                            }
                        };
                        let permit_guard = permit;
                        loop {
                            match connection.accept_bi().await {
                                Ok((send, recv)) => {
                                    counter!("mqtt_quic_stream_accept_total", "result" => "ok")
                                        .increment(1);
                                    let broker = broker.clone();
                                    let listener_profile = listener_profile.clone();
                                    tokio::spawn(async move {
                                        if let Err(error) = with_listener_profile(
                                            listener_profile,
                                            drive_session(
                                                TcpTransport::with_bandwidth(
                                                    QuicBiStream { send, recv },
                                                    ingress_limits.0.or(bandwidth.0),
                                                    ingress_limits.1.or(bandwidth.1),
                                                ),
                                                broker,
                                            ),
                                        )
                                        .await
                                        {
                                            eprintln!("greenmqtt quic session error: {error:#}");
                                        }
                                    });
                                }
                                Err(error) => {
                                    counter!("mqtt_quic_stream_accept_total", "result" => "error")
                                        .increment(1);
                                    counter!("mqtt_quic_connection_close_total", "reason" => "stream_accept_error").increment(1);
                                    eprintln!("greenmqtt quic stream accept error: {error:#}");
                                    break;
                                }
                            }
                        }
                        drop(permit_guard);
                    }
                    Err(error) => {
                        counter!("mqtt_quic_handshake_total", "result" => "error").increment(1);
                        eprintln!("greenmqtt quic handshake error: {error:#}");
                    }
                },
                Err(_) => {
                    counter!("mqtt_quic_handshake_total", "result" => "timeout").increment(1);
                    eprintln!("greenmqtt quic handshake timeout");
                }
            }
        });
    }
}

fn load_tls_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    ensure_rustls_provider_installed();
    let (certs, key) = load_tls_material(cert_path, key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_quic_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<quinn::ServerConfig> {
    ensure_rustls_provider_installed();
    let (certs, key) = load_tls_material(cert_path, key_path)?;
    Ok(quinn::ServerConfig::with_single_cert(certs, key)?)
}

pub(crate) fn ensure_rustls_provider_installed() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_tls_material(
    cert_path: &Path,
    key_path: &Path,
) -> anyhow::Result<(
    Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_pem.as_slice());
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    let mut key_reader = std::io::BufReader::new(key_pem.as_slice());
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", key_path.display()))?;
    Ok((certs, key))
}

#[cfg(test)]
mod tests {
    use super::{
        handshake_timeout, ingress_bandwidth_limits, parse_proxy_protocol_v1_header,
        parse_proxy_protocol_v2_header, websocket_path, websocket_request_allowed,
    };
    use std::time::Duration;

    #[test]
    fn parses_proxy_protocol_v1_header_length() {
        let header = b"PROXY TCP4 203.0.113.1 192.0.2.1 56324 1883\r\nmqtt";
        let parsed = parse_proxy_protocol_v1_header(header).unwrap();
        assert_eq!(parsed, Some(45));
    }

    #[test]
    fn ignores_non_proxy_prefix() {
        assert!(parse_proxy_protocol_v1_header(b"\x10\x0emqtt-connect")
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_proxy_protocol_v2_header_length() {
        let mut header = Vec::from(&b"\r\n\r\n\0\r\nQUIT\n"[..]);
        header.extend_from_slice(&[0x21, 0x11, 0x00, 0x0c]);
        header.extend_from_slice(&[203, 0, 113, 1, 192, 0, 2, 1, 0xdb, 0x34, 0x07, 0x5b]);
        header.extend_from_slice(b"mqtt");
        let parsed = parse_proxy_protocol_v2_header(&header).unwrap();
        assert_eq!(parsed, Some(28));
    }

    #[test]
    fn handshake_timeout_reads_env_override() {
        std::env::set_var("GREENMQTT_HANDSHAKE_TIMEOUT_MS", "1500");
        assert_eq!(handshake_timeout(), Duration::from_millis(1500));
        std::env::remove_var("GREENMQTT_HANDSHAKE_TIMEOUT_MS");
    }

    #[test]
    fn ingress_bandwidth_limits_read_env_override() {
        std::env::set_var("GREENMQTT_INGRESS_READ_RATE_PER_SEC", "1024");
        std::env::set_var("GREENMQTT_INGRESS_READ_BURST", "2048");
        std::env::set_var("GREENMQTT_INGRESS_WRITE_RATE_PER_SEC", "512");
        std::env::set_var("GREENMQTT_INGRESS_WRITE_BURST", "1024");
        assert_eq!(
            ingress_bandwidth_limits(),
            (Some((1024, 2048)), Some((512, 1024)))
        );
        std::env::remove_var("GREENMQTT_INGRESS_READ_RATE_PER_SEC");
        std::env::remove_var("GREENMQTT_INGRESS_READ_BURST");
        std::env::remove_var("GREENMQTT_INGRESS_WRITE_RATE_PER_SEC");
        std::env::remove_var("GREENMQTT_INGRESS_WRITE_BURST");
    }

    #[test]
    fn websocket_path_defaults_to_root_and_honors_env() {
        std::env::remove_var("GREENMQTT_WS_PATH");
        assert_eq!(websocket_path("GREENMQTT_WS_PATH"), "/");
        std::env::set_var("GREENMQTT_WS_PATH", "/mqtt");
        assert_eq!(websocket_path("GREENMQTT_WS_PATH"), "/mqtt");
        std::env::remove_var("GREENMQTT_WS_PATH");
    }

    #[test]
    fn websocket_request_allowed_requires_exact_path_match() {
        assert!(websocket_request_allowed("/mqtt", "/mqtt"));
        assert!(!websocket_request_allowed("/", "/mqtt"));
        assert!(!websocket_request_allowed("/mqtt/extra", "/mqtt"));
    }
}
