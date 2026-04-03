use greenmqtt_plugin_api::{AclProvider, AuthProvider, EventHook};
use quinn::{Endpoint, RecvStream, SendStream};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Once;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_async;

use super::session::{drive_session, TcpTransport, WsTransport};
use super::BrokerRuntime;

static RUSTLS_PROVIDER: Once = Once::new();

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

pub async fn serve_tcp<A, C, H>(
    broker: Arc<BrokerRuntime<A, C, H>>,
    bind: SocketAddr,
) -> anyhow::Result<()>
where
    A: AuthProvider + 'static,
    C: AclProvider + 'static,
    H: EventHook + 'static,
{
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let broker = broker.clone();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            if let Err(error) = drive_session(TcpTransport::new(stream), broker).await {
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
    let listener = TcpListener::bind(bind).await?;
    let acceptor = load_tls_acceptor(cert_path.as_ref(), key_path.as_ref())?;
    loop {
        let (stream, _) = listener.accept().await?;
        let broker = broker.clone();
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
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    if let Err(error) = drive_session(TcpTransport::new(stream), broker).await {
                        eprintln!("greenmqtt tls session error: {error:#}");
                    }
                }
                Err(error) => {
                    eprintln!("greenmqtt tls handshake error: {error:#}");
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
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let broker = broker.clone();
        let permit = match broker.try_acquire_connection_slot() {
            Ok(permit) => permit,
            Err(()) => {
                drop(stream);
                continue;
            }
        };
        tokio::spawn(async move {
            let permit_guard = permit;
            match accept_async(stream).await {
                Ok(stream) => {
                    if let Err(error) = drive_session(WsTransport::new(stream), broker).await {
                        eprintln!("greenmqtt ws session error: {error:#}");
                    }
                }
                Err(error) => {
                    eprintln!("greenmqtt ws handshake error: {error:#}");
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
    let listener = TcpListener::bind(bind).await?;
    let acceptor = load_tls_acceptor(cert_path.as_ref(), key_path.as_ref())?;
    loop {
        let (stream, _) = listener.accept().await?;
        let broker = broker.clone();
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
            match acceptor.accept(stream).await {
                Ok(stream) => match accept_async(stream).await {
                    Ok(stream) => {
                        if let Err(error) = drive_session(WsTransport::new(stream), broker).await {
                            eprintln!("greenmqtt wss session error: {error:#}");
                        }
                    }
                    Err(error) => {
                        eprintln!("greenmqtt wss websocket handshake error: {error:#}");
                    }
                },
                Err(error) => {
                    eprintln!("greenmqtt wss tls handshake error: {error:#}");
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
    let endpoint = Endpoint::server(
        load_quic_server_config(cert_path.as_ref(), key_path.as_ref())?,
        bind,
    )?;
    loop {
        let Some(incoming) = endpoint.accept().await else {
            return Ok(());
        };
        let broker = broker.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    let permit = match broker.try_acquire_connection_slot() {
                        Ok(permit) => permit,
                        Err(()) => {
                            connection.close(0u32.into(), b"connection limit exceeded");
                            return;
                        }
                    };
                    let permit_guard = permit;
                    loop {
                        match connection.accept_bi().await {
                            Ok((send, recv)) => {
                                let broker = broker.clone();
                                tokio::spawn(async move {
                                    if let Err(error) = drive_session(
                                        TcpTransport::new(QuicBiStream { send, recv }),
                                        broker,
                                    )
                                    .await
                                    {
                                        eprintln!("greenmqtt quic session error: {error:#}");
                                    }
                                });
                            }
                            Err(error) => {
                                eprintln!("greenmqtt quic stream accept error: {error:#}");
                                break;
                            }
                        }
                    }
                    drop(permit_guard);
                }
                Err(error) => {
                    eprintln!("greenmqtt quic handshake error: {error:#}");
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
