use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_greenmqtt-cli")
}

fn reserve_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn wait_for_port(addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("port {addr} did not open in time");
}

fn websocket_handshake(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!(
        "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = Vec::new();
    let mut chunk = [0u8; 512];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0, "websocket handshake closed unexpectedly");
        response.extend_from_slice(&chunk[..read]);
    }
    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 101"),
        "unexpected ws handshake response: {response}"
    );
    stream
}

fn send_ws_binary(stream: &mut TcpStream, payload: &[u8]) {
    let mut frame = Vec::with_capacity(payload.len() + 16);
    frame.push(0x82);
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    let mask = [0x12, 0x34, 0x56, 0x78];
    frame.extend_from_slice(&mask);
    for (index, byte) in payload.iter().enumerate() {
        frame.push(byte ^ mask[index % 4]);
    }
    stream.write_all(&frame).unwrap();
}

fn read_ws_binary(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x0f, 0x02, "expected websocket binary frame");
    let mut len = (header[1] & 0x7f) as usize;
    if len == 126 {
        let mut extended = [0u8; 2];
        stream.read_exact(&mut extended).unwrap();
        len = u16::from_be_bytes(extended) as usize;
    }
    let masked = header[1] & 0x80 != 0;
    let mut mask = [0u8; 4];
    if masked {
        stream.read_exact(&mut mask).unwrap();
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).unwrap();
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    payload
}

fn connect_packet_with_username(client_id: &str, username: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(0b1000_0010);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(username.len() as u16).to_be_bytes());
    body.extend_from_slice(username.as_bytes());
    packet(0x10, &body)
}

fn connect_packet_v5_with_username(client_id: &str, username: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0, 4]);
    body.extend_from_slice(b"MQTT");
    body.push(5);
    body.push(0b1000_0010);
    body.extend_from_slice(&30u16.to_be_bytes());
    body.push(0);
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(username.len() as u16).to_be_bytes());
    body.extend_from_slice(username.as_bytes());
    packet(0x10, &body)
}

fn publish_packet_v5_qos1(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(&1u16.to_be_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    packet(0x32, &body)
}

fn publish_packet(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    body.extend_from_slice(payload);
    packet(0x30, &body)
}

fn subscribe_packet(packet_id: u16, topic_filter: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.extend_from_slice(&(topic_filter.len() as u16).to_be_bytes());
    body.extend_from_slice(topic_filter.as_bytes());
    body.push(0x01);
    packet(0x82, &body)
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut packet = vec![header];
    encode_remaining_length(&mut packet, body.len());
    packet.extend_from_slice(body);
    packet
}

fn encode_remaining_length(target: &mut Vec<u8>, mut value: usize) {
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded |= 0x80;
        }
        target.push(encoded);
        if value == 0 {
            break;
        }
    }
}

fn read_packet_type(stream: &mut TcpStream) -> u8 {
    let mut byte = [0u8; 1];
    stream.read_exact(&mut byte).unwrap();
    byte[0] >> 4
}

fn read_packet_body(stream: &mut TcpStream) -> Vec<u8> {
    let remaining = read_remaining_length(stream);
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).unwrap();
    body
}

fn read_remaining_length(stream: &mut TcpStream) -> usize {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        value += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            return value;
        }
        multiplier *= 128;
    }
}

fn connect_return_code(addr: SocketAddr, client_id: &str, username: &str) -> u8 {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&connect_packet_with_username(client_id, username))
        .unwrap();
    stream.shutdown(Shutdown::Write).unwrap();
    assert_eq!(read_packet_type(&mut stream), 2);
    let remaining = read_remaining_length(&mut stream);
    let mut body = vec![0u8; remaining];
    stream.read_exact(&mut body).unwrap();
    body[1]
}

fn ws_connect_return_code(addr: SocketAddr, client_id: &str, username: &str) -> u8 {
    let mut stream = websocket_handshake(addr);
    send_ws_binary(
        &mut stream,
        &connect_packet_with_username(client_id, username),
    );
    let body = read_ws_binary(&mut stream);
    assert_eq!(body[0] >> 4, 2);
    body[3]
}

fn mqtt5_connect(addr: SocketAddr, client_id: &str, username: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&connect_packet_v5_with_username(client_id, username))
        .unwrap();
    assert_eq!(read_packet_type(&mut stream), 2);
    let _connack = read_packet_body(&mut stream);
    stream
}

fn mqtt3_connect(addr: SocketAddr, client_id: &str, username: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&connect_packet_with_username(client_id, username))
        .unwrap();
    assert_eq!(read_packet_type(&mut stream), 2);
    let _connack = read_packet_body(&mut stream);
    stream
}

fn read_puback_reason_code(stream: &mut TcpStream) -> u8 {
    assert_eq!(read_packet_type(stream), 4);
    let body = read_packet_body(stream);
    body[2]
}

fn read_publish_payload(stream: &mut TcpStream) -> Vec<u8> {
    assert_eq!(read_packet_type(stream), 3);
    let body = read_packet_body(stream);
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut cursor = 2 + topic_len;
    if body[0] & 0b0000_0110 != 0 {
        cursor += 2;
    }
    body[cursor..].to_vec()
}

fn parse_ws_publish_topic(packet: &[u8]) -> String {
    assert_eq!(packet[0] >> 4, 3);
    let mut cursor = 1usize;
    let mut multiplier = 1usize;
    loop {
        let byte = packet[cursor];
        cursor += 1;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        assert!(multiplier < 128 * 128 * 128);
    }
    let topic_len = u16::from_be_bytes([packet[cursor], packet[cursor + 1]]) as usize;
    cursor += 2;
    std::str::from_utf8(&packet[cursor..cursor + topic_len])
        .unwrap()
        .to_string()
}

struct ServeProcess {
    _log_file: tempfile::NamedTempFile,
    child: Child,
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_multi_listener_process(
    http_bind: SocketAddr,
    rpc_bind: SocketAddr,
    mqtt_default: SocketAddr,
    mqtt_guest: SocketAddr,
) -> ServeProcess {
    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env(
            "GREENMQTT_MQTT_BINDS",
            format!("{mqtt_default},{}@guest", mqtt_guest),
        )
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env("GREENMQTT_AUTH_IDENTITIES__GUEST", "tenant-b:*:*")
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WS_BIND")
        .env_remove("GREENMQTT_WS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();

    ServeProcess {
        _log_file: log_file,
        child,
    }
}

fn start_ws_multi_listener_process(
    http_bind: SocketAddr,
    rpc_bind: SocketAddr,
    ws_default: SocketAddr,
    ws_guest: SocketAddr,
) -> ServeProcess {
    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env(
            "GREENMQTT_WS_BINDS",
            format!("{ws_default},{}@guest", ws_guest),
        )
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env("GREENMQTT_AUTH_IDENTITIES__GUEST", "tenant-b:*:*")
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_MQTT_BIND")
        .env_remove("GREENMQTT_MQTT_BINDS")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();

    ServeProcess {
        _log_file: log_file,
        child,
    }
}

#[test]
fn mqtt_tcp_multi_listener_profiles_route_auth_independently() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let mqtt_default = reserve_addr();
    let mqtt_guest = reserve_addr();

    let _server = start_multi_listener_process(http_bind, rpc_bind, mqtt_default, mqtt_guest);
    wait_for_port(mqtt_default, Duration::from_secs(10));
    wait_for_port(mqtt_guest, Duration::from_secs(10));

    assert_eq!(
        connect_return_code(mqtt_default, "default-ok", "tenant-a:alice"),
        0
    );
    assert_eq!(
        connect_return_code(mqtt_default, "default-deny", "tenant-b:bob"),
        5
    );
    assert_eq!(
        connect_return_code(mqtt_guest, "guest-ok", "tenant-b:bob"),
        0
    );
    assert_eq!(
        connect_return_code(mqtt_guest, "guest-deny", "tenant-a:alice"),
        5
    );
}

#[test]
fn mqtt_tcp_multi_listener_profiles_can_share_default_auth_profile() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let mqtt_a = reserve_addr();
    let mqtt_b = reserve_addr();

    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env("GREENMQTT_MQTT_BINDS", format!("{mqtt_a},{mqtt_b}"))
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WS_BIND")
        .env_remove("GREENMQTT_WS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let _server = ServeProcess {
        _log_file: log_file,
        child,
    };

    wait_for_port(mqtt_a, Duration::from_secs(10));
    wait_for_port(mqtt_b, Duration::from_secs(10));

    assert_eq!(connect_return_code(mqtt_a, "shared-a", "tenant-a:alice"), 0);
    assert_eq!(connect_return_code(mqtt_b, "shared-b", "tenant-a:bob"), 0);
    assert_eq!(
        connect_return_code(mqtt_a, "shared-deny-a", "tenant-b:mallory"),
        5
    );
    assert_eq!(
        connect_return_code(mqtt_b, "shared-deny-b", "tenant-b:mallory"),
        5
    );
}

#[test]
fn mqtt_ws_multi_listener_profiles_route_auth_independently() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let ws_default = reserve_addr();
    let ws_guest = reserve_addr();

    let _server = start_ws_multi_listener_process(http_bind, rpc_bind, ws_default, ws_guest);
    wait_for_port(ws_default, Duration::from_secs(10));
    wait_for_port(ws_guest, Duration::from_secs(10));

    assert_eq!(
        ws_connect_return_code(ws_default, "ws-default-ok", "tenant-a:alice"),
        0
    );
    assert_eq!(
        ws_connect_return_code(ws_default, "ws-default-deny", "tenant-b:bob"),
        5
    );
    assert_eq!(
        ws_connect_return_code(ws_guest, "ws-guest-ok", "tenant-b:bob"),
        0
    );
    assert_eq!(
        ws_connect_return_code(ws_guest, "ws-guest-deny", "tenant-a:alice"),
        5
    );
}

#[test]
fn mqtt_ws_multi_listener_profiles_can_share_default_auth_profile() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let ws_a = reserve_addr();
    let ws_b = reserve_addr();

    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env("GREENMQTT_WS_BINDS", format!("{ws_a},{ws_b}"))
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_MQTT_BIND")
        .env_remove("GREENMQTT_MQTT_BINDS")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let _server = ServeProcess {
        _log_file: log_file,
        child,
    };

    wait_for_port(ws_a, Duration::from_secs(10));
    wait_for_port(ws_b, Duration::from_secs(10));

    assert_eq!(
        ws_connect_return_code(ws_a, "ws-shared-a", "tenant-a:alice"),
        0
    );
    assert_eq!(
        ws_connect_return_code(ws_b, "ws-shared-b", "tenant-a:bob"),
        0
    );
    assert_eq!(
        ws_connect_return_code(ws_a, "ws-shared-deny-a", "tenant-b:mallory"),
        5
    );
    assert_eq!(
        ws_connect_return_code(ws_b, "ws-shared-deny-b", "tenant-b:mallory"),
        5
    );
}

#[test]
fn mqtt_ws_multi_listener_profiles_route_hooks_independently() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let ws_default = reserve_addr();
    let ws_guest = reserve_addr();

    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env(
            "GREENMQTT_WS_BINDS",
            format!("{ws_default},{}@guest", ws_guest),
        )
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env("GREENMQTT_AUTH_IDENTITIES__GUEST", "tenant-b:*:*")
        .env(
            "GREENMQTT_TOPIC_REWRITE_RULES",
            "tenant-a@devices/+/state=default/{1}",
        )
        .env(
            "GREENMQTT_TOPIC_REWRITE_RULES__GUEST",
            "tenant-b@devices/+/state=guest/{1}",
        )
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_MQTT_BIND")
        .env_remove("GREENMQTT_MQTT_BINDS")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let _server = ServeProcess {
        _log_file: log_file,
        child,
    };

    wait_for_port(ws_default, Duration::from_secs(10));
    wait_for_port(ws_guest, Duration::from_secs(10));

    let mut default_sub = websocket_handshake(ws_default);
    send_ws_binary(
        &mut default_sub,
        &connect_packet_with_username("ws-default-sub", "tenant-a:alice"),
    );
    let connack = read_ws_binary(&mut default_sub);
    assert_eq!(connack[0] >> 4, 2);
    send_ws_binary(&mut default_sub, &subscribe_packet(1, "default/+"));
    let suback = read_ws_binary(&mut default_sub);
    assert_eq!(suback[0] >> 4, 9);

    let mut guest_sub = websocket_handshake(ws_guest);
    send_ws_binary(
        &mut guest_sub,
        &connect_packet_with_username("ws-guest-sub", "tenant-b:bob"),
    );
    let connack = read_ws_binary(&mut guest_sub);
    assert_eq!(connack[0] >> 4, 2);
    send_ws_binary(&mut guest_sub, &subscribe_packet(1, "guest/+"));
    let suback = read_ws_binary(&mut guest_sub);
    assert_eq!(suback[0] >> 4, 9);

    let mut default_pub = websocket_handshake(ws_default);
    send_ws_binary(
        &mut default_pub,
        &connect_packet_with_username("ws-default-pub", "tenant-a:alice"),
    );
    let connack = read_ws_binary(&mut default_pub);
    assert_eq!(connack[0] >> 4, 2);
    send_ws_binary(
        &mut default_pub,
        &publish_packet("devices/d1/state", b"default"),
    );
    let payload = read_ws_binary(&mut default_sub);
    let topic = parse_ws_publish_topic(&payload);
    assert_eq!(topic, "default/d1");

    let mut guest_pub = websocket_handshake(ws_guest);
    send_ws_binary(
        &mut guest_pub,
        &connect_packet_with_username("ws-guest-pub", "tenant-b:bob"),
    );
    let connack = read_ws_binary(&mut guest_pub);
    assert_eq!(connack[0] >> 4, 2);
    send_ws_binary(
        &mut guest_pub,
        &publish_packet("devices/d2/state", b"guest"),
    );
    let payload = read_ws_binary(&mut guest_sub);
    let topic = parse_ws_publish_topic(&payload);
    assert_eq!(topic, "guest/d2");
}

#[test]
fn mqtt_tcp_multi_listener_profiles_route_acl_independently() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let mqtt_default = reserve_addr();
    let mqtt_guest = reserve_addr();

    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env(
            "GREENMQTT_MQTT_BINDS",
            format!("{mqtt_default},{}@guest", mqtt_guest),
        )
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env(
            "GREENMQTT_ACL_RULES",
            "allow-pub@tenant-a:alice:*=devices/#",
        )
        .env(
            "GREENMQTT_ACL_RULES__GUEST",
            "allow-pub@tenant-a:alice:*=public/#",
        )
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WS_BIND")
        .env_remove("GREENMQTT_WS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let _server = ServeProcess {
        _log_file: log_file,
        child,
    };

    wait_for_port(mqtt_default, Duration::from_secs(10));
    wait_for_port(mqtt_guest, Duration::from_secs(10));

    let mut default_client = mqtt5_connect(mqtt_default, "default-pub", "tenant-a:alice");
    default_client
        .write_all(&publish_packet_v5_qos1("devices/d1/state", b"default"))
        .unwrap();
    assert_eq!(read_puback_reason_code(&mut default_client), 0x00);

    let mut guest_client = mqtt5_connect(mqtt_guest, "guest-pub", "tenant-a:alice");
    guest_client
        .write_all(&publish_packet_v5_qos1("devices/d1/state", b"guest"))
        .unwrap();
    assert_eq!(read_puback_reason_code(&mut guest_client), 0x87);
}

#[test]
fn mqtt_tcp_multi_listener_profiles_route_hooks_independently() {
    let http_bind = reserve_addr();
    let rpc_bind = reserve_addr();
    let mqtt_default = reserve_addr();
    let mqtt_guest = reserve_addr();

    let log_file = tempfile::NamedTempFile::new().unwrap();
    let log = log_file.reopen().unwrap();
    let child = Command::new(binary_path())
        .arg("serve")
        .env("GREENMQTT_NODE_ID", "1")
        .env("GREENMQTT_HTTP_BIND", http_bind.to_string())
        .env("GREENMQTT_RPC_BIND", rpc_bind.to_string())
        .env(
            "GREENMQTT_MQTT_BINDS",
            format!("{mqtt_default},{}@guest", mqtt_guest),
        )
        .env("GREENMQTT_AUTH_IDENTITIES", "tenant-a:*:*")
        .env("GREENMQTT_AUTH_IDENTITIES__GUEST", "tenant-b:*:*")
        .env(
            "GREENMQTT_TOPIC_REWRITE_RULES",
            "tenant-a@devices/+/state=default/{1}",
        )
        .env(
            "GREENMQTT_TOPIC_REWRITE_RULES__GUEST",
            "tenant-b@devices/+/state=guest/{1}",
        )
        .env_remove("GREENMQTT_STATE_ENDPOINT")
        .env_remove("GREENMQTT_TLS_BIND")
        .env_remove("GREENMQTT_TLS_BINDS")
        .env_remove("GREENMQTT_WS_BIND")
        .env_remove("GREENMQTT_WS_BINDS")
        .env_remove("GREENMQTT_WSS_BIND")
        .env_remove("GREENMQTT_WSS_BINDS")
        .env_remove("GREENMQTT_QUIC_BIND")
        .env_remove("GREENMQTT_QUIC_BINDS")
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let _server = ServeProcess {
        _log_file: log_file,
        child,
    };

    wait_for_port(mqtt_default, Duration::from_secs(10));
    wait_for_port(mqtt_guest, Duration::from_secs(10));

    let mut default_sub = mqtt3_connect(mqtt_default, "default-sub", "tenant-a:alice");
    default_sub
        .write_all(&subscribe_packet(1, "default/+"))
        .unwrap();
    assert_eq!(read_packet_type(&mut default_sub), 9);
    let _ = read_packet_body(&mut default_sub);

    let mut guest_sub = mqtt3_connect(mqtt_guest, "guest-sub", "tenant-b:bob");
    guest_sub
        .write_all(&subscribe_packet(1, "guest/+"))
        .unwrap();
    assert_eq!(read_packet_type(&mut guest_sub), 9);
    let _ = read_packet_body(&mut guest_sub);

    let mut default_pub = mqtt3_connect(mqtt_default, "default-pub", "tenant-a:alice");
    default_pub
        .write_all(&publish_packet("devices/d1/state", b"default"))
        .unwrap();
    assert_eq!(read_publish_payload(&mut default_sub), b"default");

    let mut guest_pub = mqtt3_connect(mqtt_guest, "guest-pub", "tenant-b:bob");
    guest_pub
        .write_all(&publish_packet("devices/d2/state", b"guest"))
        .unwrap();
    assert_eq!(read_publish_payload(&mut guest_sub), b"guest");
}
