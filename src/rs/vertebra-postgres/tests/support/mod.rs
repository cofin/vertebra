//! Loopback scripted PostgreSQL wire server helpers for `vertebra-postgres` integration tests.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Encode a backend `AuthenticationOk` (`'R'`, code `0`) frame.
pub fn frame_auth_ok() -> Vec<u8> {
    vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0]
}

/// Encode a backend `AuthenticationCleartextPassword` (`'R'`, code `3`) frame.
pub fn frame_auth_cleartext() -> Vec<u8> {
    vec![b'R', 0, 0, 0, 8, 0, 0, 0, 3]
}

/// Encode a backend `AuthenticationMD5Password` (`'R'`, code `5`, 4-byte salt) frame.
pub fn frame_auth_md5(salt: [u8; 4]) -> Vec<u8> {
    let mut out = vec![b'R', 0, 0, 0, 12, 0, 0, 0, 5];
    out.extend_from_slice(&salt);
    out
}

/// Encode a backend `AuthenticationGSS` (`'R'`, code `7`) frame.
pub fn frame_auth_gss() -> Vec<u8> {
    vec![b'R', 0, 0, 0, 8, 0, 0, 0, 7]
}

/// Encode a backend `AuthenticationSSPI` (`'R'`, code `9`) frame.
pub fn frame_auth_sspi() -> Vec<u8> {
    vec![b'R', 0, 0, 0, 8, 0, 0, 0, 9]
}

/// Encode a backend `AuthenticationKerberosV5` (`'R'`, code `2`) frame.
pub fn frame_auth_kerberos() -> Vec<u8> {
    vec![b'R', 0, 0, 0, 8, 0, 0, 0, 2]
}

/// Encode a backend `AuthenticationSASL` (`'R'`, code `10`, mechanism list) frame.
pub fn frame_auth_sasl(mechanisms: &[&[u8]]) -> Vec<u8> {
    let mut payload = vec![0, 0, 0, 10];
    for m in mechanisms {
        payload.extend_from_slice(m);
        payload.push(0);
    }
    payload.push(0);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'R'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Encode a backend `AuthenticationSASLContinue` (`'R'`, code `11`) frame.
pub fn frame_auth_sasl_continue(data: &[u8]) -> Vec<u8> {
    let mut payload = vec![0, 0, 0, 11];
    payload.extend_from_slice(data);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'R'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Encode a backend `AuthenticationSASLFinal` (`'R'`, code `12`) frame.
pub fn frame_auth_sasl_final(data: &[u8]) -> Vec<u8> {
    let mut payload = vec![0, 0, 0, 12];
    payload.extend_from_slice(data);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'R'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Encode a backend `ParameterStatus` (`'S'`) frame.
pub fn frame_parameter_status(name: &str, value: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(name.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'S'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Encode a backend `BackendKeyData` (`'K'`) frame.
pub fn frame_backend_key_data(process_id: i32, secret_key: i32) -> Vec<u8> {
    let mut out = vec![b'K', 0, 0, 0, 12];
    out.extend_from_slice(&process_id.to_be_bytes());
    out.extend_from_slice(&secret_key.to_be_bytes());
    out
}

/// Encode a backend `ReadyForQuery` (`'Z'`) frame.
pub fn frame_ready_for_query(status: u8) -> Vec<u8> {
    vec![b'Z', 0, 0, 0, 5, status]
}

/// Encode a backend `NoticeResponse` (`'N'`) frame.
pub fn frame_notice_response(message: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"SNOTICE\0VNOTICE\0C00000\0M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'N'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Encode a backend `ErrorResponse` (`'E'`) frame.
pub fn frame_error_response(sqlstate: &str, message: &str, detail: Option<&str>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"SERROR\0VFATAL\0C");
    payload.extend_from_slice(sqlstate.as_bytes());
    payload.push(0);
    payload.push(b'M');
    payload.extend_from_slice(message.as_bytes());
    payload.push(0);
    if let Some(d) = detail {
        payload.push(b'D');
        payload.extend_from_slice(d.as_bytes());
        payload.push(0);
    }
    payload.push(0);
    let len = (4 + payload.len() as u32).to_be_bytes();
    let mut out = vec![b'E'];
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    out
}

/// Read an untagged frontend `StartupMessage` (`[len: i32, proto: 0x00030000, k\0v\0...\0]`).
pub async fn read_startup_packet<S>(stream: &mut S) -> io::Result<BTreeMap<String, String>>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    let total_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let proto = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    if proto != 0x0003_0000 || total_len < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected startup header len={total_len} proto=0x{proto:08x}"),
        ));
    }
    let mut body = vec![0u8; total_len - 8];
    stream.read_exact(&mut body).await?;
    let mut params = BTreeMap::new();
    let parts: Vec<&[u8]> = body.split(|b| *b == 0).collect();
    let mut i = 0;
    while i + 1 < parts.len() && !parts[i].is_empty() {
        let k = String::from_utf8_lossy(parts[i]).into_owned();
        let v = String::from_utf8_lossy(parts[i + 1]).into_owned();
        params.insert(k, v);
        i += 2;
    }
    Ok(params)
}

/// Read a tagged frontend message (`[tag: u8, len: i32, payload...]`).
pub async fn read_tagged_frontend_frame<S>(stream: &mut S) -> io::Result<(u8, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let tag = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid frontend frame len {len}"),
        ));
    }
    let mut payload = vec![0u8; len - 4];
    stream.read_exact(&mut payload).await?;
    Ok((tag, payload))
}

/// Read an initial 8-byte `SSLRequest` packet (`[0, 0, 0, 8, 4, 210, 22, 47]`).
pub async fn read_ssl_request<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncRead + Unpin,
{
    let mut buf = [0u8; 8];
    stream.read_exact(&mut buf).await?;
    if buf != [0, 0, 0, 8, 4, 210, 22, 47] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected SSLRequest, got {buf:?}"),
        ));
    }
    Ok(())
}

/// Spawn a single-accept loopback TCP server and return `(SocketAddr, Arc<AtomicUsize>, JoinHandle)`.
pub async fn spawn_scripted_server<F, Fut>(
    handler: F,
) -> (
    SocketAddr,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
)
where
    F: FnOnce(TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener local_addr");
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_clone = Arc::clone(&accepts);
    let handle = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            accepts_clone.fetch_add(1, Ordering::SeqCst);
            let _ = stream.set_nodelay(true);
            handler(stream).await;
        }
    });
    (addr, accepts, handle)
}

/// Write a sequence of frames and flush.
pub async fn write_frames<S>(stream: &mut S, frames: &[Vec<u8>]) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    for f in frames {
        stream.write_all(f).await?;
    }
    stream.flush().await
}

/// Parsed frontend `SASLInitialResponse` (`'p'`) payload.
#[derive(Debug, Clone)]
pub struct ParsedSaslInitial {
    pub mechanism: String,
    pub gs2_header: String,
    pub client_first_bare: String,
    pub client_nonce: String,
}

/// Parse a `SASLInitialResponse` (`'p'`) payload into mechanism, GS2 header, bare client-first, and nonce.
pub fn parse_sasl_initial(payload: &[u8]) -> ParsedSaslInitial {
    let nul_pos = payload
        .iter()
        .position(|b| *b == 0)
        .expect("SASLInitialResponse mechanism NUL");
    let mechanism = String::from_utf8(payload[..nul_pos].to_vec()).expect("SASL mechanism utf8");
    let rest = &payload[nul_pos + 1..];
    assert!(rest.len() >= 4, "SASLInitialResponse length prefix");
    let data_len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    assert_eq!(rest.len() - 4, data_len, "SASLInitialResponse data length");
    let msg = String::from_utf8(rest[4..].to_vec()).expect("SASLInitialResponse utf8");
    let bare_idx = msg
        .find("n=,r=")
        .expect("SCRAM client-first-message must contain n=,r=");
    let gs2_header = msg[..bare_idx].to_string();
    let client_first_bare = msg[bare_idx..].to_string();
    let client_nonce = client_first_bare
        .strip_prefix("n=,r=")
        .expect("n=,r= prefix")
        .to_string();
    ParsedSaslInitial {
        mechanism,
        gs2_header,
        client_first_bare,
        client_nonce,
    }
}

/// Compute PBKDF2-HMAC-SHA256 (`Hi`) and verify client `SASLResponse`, returning `v=<base64>` server-final.
pub fn scram_verify_and_sign(
    normalized_password: &[u8],
    initial: &ParsedSaslInitial,
    server_first: &str,
    salt: &[u8],
    iterations: u32,
    expected_cbind_data: &[u8],
    client_final: &str,
) -> String {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    let (without_proof, proof_b64) = client_final
        .rsplit_once(",p=")
        .expect("client-final-message must end with ,p=");

    let mut expected_c = initial.gs2_header.as_bytes().to_vec();
    expected_c.extend_from_slice(expected_cbind_data);
    let expected_c_prefix = format!("c={},r=", STANDARD.encode(&expected_c));
    assert!(
        without_proof.starts_with(&expected_c_prefix),
        "unexpected channel binding prefix in {without_proof}, expected {expected_c_prefix}"
    );

    let mut u_prev = {
        let mut mac = Hmac::<Sha256>::new_from_slice(normalized_password).unwrap();
        mac.update(salt);
        mac.update(&[0, 0, 0, 1]);
        mac.finalize().into_bytes()
    };
    let mut salted_password = u_prev;
    for _ in 1..iterations {
        let mut mac = Hmac::<Sha256>::new_from_slice(normalized_password).unwrap();
        mac.update(&u_prev);
        u_prev = mac.finalize().into_bytes();
        for (a, b) in salted_password.iter_mut().zip(u_prev.iter()) {
            *a ^= *b;
        }
    }

    let client_key = {
        let mut mac = Hmac::<Sha256>::new_from_slice(&salted_password).unwrap();
        mac.update(b"Client Key");
        mac.finalize().into_bytes()
    };
    let stored_key = Sha256::digest(client_key);
    let auth_message = format!(
        "{},{},{}",
        initial.client_first_bare, server_first, without_proof
    );
    let client_sig = {
        let mut mac = Hmac::<Sha256>::new_from_slice(&stored_key).unwrap();
        mac.update(auth_message.as_bytes());
        mac.finalize().into_bytes()
    };
    let mut expected_proof = client_key;
    for (p, s) in expected_proof.iter_mut().zip(client_sig.iter()) {
        *p ^= *s;
    }
    assert_eq!(
        proof_b64,
        STANDARD.encode(expected_proof),
        "client SCRAM proof mismatch"
    );

    let server_key = {
        let mut mac = Hmac::<Sha256>::new_from_slice(&salted_password).unwrap();
        mac.update(b"Server Key");
        mac.finalize().into_bytes()
    };
    let server_sig = {
        let mut mac = Hmac::<Sha256>::new_from_slice(&server_key).unwrap();
        mac.update(auth_message.as_bytes());
        mac.finalize().into_bytes()
    };
    format!("v={}", STANDARD.encode(server_sig))
}
