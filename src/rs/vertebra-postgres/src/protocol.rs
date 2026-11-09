//! Bounded PostgreSQL Protocol 3.0 wire framing, startup packet encoding, and `ErrorResponse`
//! diagnostic field extraction.

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{ErrorFields, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::Instant;

use crate::config::PostgresConfig;
use crate::errors::{PostgresError, ServerError};

/// Maximum backend frame length accepted before rejecting as a protocol violation (`16 MiB`).
pub const MAX_BACKEND_FRAME_LENGTH: usize = 16_777_216;

/// Canonical 5-byte PostgreSQL `Terminate` (`'X'`) frontend frame.
pub const TERMINATE_FRAME: [u8; 5] = [b'X', 0, 0, 0, 4];

/// Encode a Protocol 3.0 `StartupMessage` (`0x0003_0000`) from a validated [`PostgresConfig`].
pub fn encode_startup_frame(
    config: &PostgresConfig,
    out: &mut BytesMut,
) -> Result<(), PostgresError> {
    let gucs = config.startup_gucs();
    let mut params: Vec<(&str, &str)> = Vec::with_capacity(3 + gucs.len());
    params.push(("user", config.user.as_str()));
    params.push(("database", config.database.as_str()));
    params.push(("client_encoding", "UTF8"));
    for (k, v) in &gucs {
        params.push((k.as_str(), v.as_str()));
    }
    frontend::startup_message(params, out)
        .map_err(|e| PostgresError::Protocol(format!("failed to encode StartupMessage: {e}")))
}

/// Extract structured `ServerError` fields from a backend `ErrorResponse` body.
pub fn parse_error_fields(mut fields: ErrorFields<'_>) -> Result<PostgresError, PostgresError> {
    let mut severity_v: Option<String> = None;
    let mut severity_s: Option<String> = None;
    let mut sqlstate: Option<String> = None;
    let mut message: Option<String> = None;
    let mut diagnostics: Vec<(u8, String)> = Vec::new();

    while let Some(field) = fields
        .next()
        .map_err(|e| PostgresError::Protocol(format!("malformed ErrorResponse field: {e}")))?
    {
        let tag = field.type_();
        let val = String::from_utf8_lossy(field.value_bytes()).into_owned();
        match tag {
            b'V' => severity_v = Some(val),
            b'S' => severity_s = Some(val),
            b'C' => sqlstate = Some(val),
            b'M' => message = Some(val),
            other => diagnostics.push((other, val)),
        }
    }

    let severity = severity_v
        .or(severity_s)
        .unwrap_or_else(|| "ERROR".to_string());
    let code = sqlstate.ok_or_else(|| {
        PostgresError::Protocol("ErrorResponse is missing mandatory SQLSTATE ('C') field".into())
    })?;
    let primary = message.ok_or_else(|| {
        PostgresError::Protocol("ErrorResponse is missing mandatory message ('M') field".into())
    })?;
    PostgresError::server(severity, &code, primary, diagnostics)
}

/// Convert a parsed [`ServerError`] into [`PostgresError::Server`].
pub fn into_server_error(err: ServerError) -> PostgresError {
    PostgresError::Server(err)
}

/// Read and decode one bounded PostgreSQL backend [`Message`] from `stream`.
///
/// Inspects the 5-byte `[tag, len_be_u32]` header in `buf` before reserving or reading the frame
/// body, rejecting `len < 4` or `len > MAX_BACKEND_FRAME_LENGTH` (`16 MiB`) immediately with
/// [`PostgresError::Protocol`] without allocating the advertised length.
pub async fn read_message<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    deadline: Option<Instant>,
) -> Result<Message, PostgresError>
where
    S: AsyncRead + Unpin,
{
    loop {
        if buf.len() >= 5 {
            let raw_len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if !(4..=MAX_BACKEND_FRAME_LENGTH).contains(&raw_len) {
                return Err(PostgresError::Protocol(format!(
                    "backend frame length {raw_len} is outside valid bounds 4..={MAX_BACKEND_FRAME_LENGTH}"
                )));
            }
            let total_frame_bytes = 1 + raw_len;
            if buf.len() >= total_frame_bytes {
                match Message::parse(buf) {
                    Ok(Some(msg)) => return Ok(msg),
                    Ok(None) => {
                        return Err(PostgresError::Protocol(
                            "backend frame header matched buffered length but Message::parse returned None"
                                .to_string(),
                        ));
                    }
                    Err(err) => {
                        return Err(PostgresError::Protocol(format!(
                            "malformed backend frame: {err}"
                        )));
                    }
                }
            }
            let missing = total_frame_bytes - buf.len();
            buf.reserve(missing.min(64 * 1024));
        } else {
            buf.reserve(4096);
        }

        let read_res = match deadline {
            Some(dl) => {
                if Instant::now() >= dl {
                    return Err(PostgresError::Transport(
                        "PostgreSQL frame read timed out".to_string(),
                    ));
                }
                tokio::time::timeout_at(dl, stream.read_buf(buf))
                    .await
                    .map_err(|_| {
                        PostgresError::Transport("PostgreSQL frame read timed out".to_string())
                    })?
            }
            None => stream.read_buf(buf).await,
        };

        let n = read_res.map_err(|e| PostgresError::Transport(format!("socket read error: {e}")))?;
        if n == 0 {
            return Err(PostgresError::Transport(
                "unexpected EOF from PostgreSQL backend".to_string(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RawPostgresOptions;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    struct ChunkedReader {
        chunks: Vec<Vec<u8>>,
        idx: usize,
        offset: usize,
    }

    impl ChunkedReader {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks,
                idx: 0,
                offset: 0,
            }
        }
    }

    impl AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            while self.idx < self.chunks.len() {
                let chunk_len = self.chunks[self.idx].len();
                if self.offset < chunk_len {
                    let to_copy = (chunk_len - self.offset).min(buf.remaining());
                    let start = self.offset;
                    let slice = self.chunks[self.idx][start..start + to_copy].to_vec();
                    buf.put_slice(&slice);
                    self.offset += to_copy;
                    if self.offset == chunk_len {
                        self.idx += 1;
                        self.offset = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                self.idx += 1;
                self.offset = 0;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn startup_frame_encodes_protocol_v3_and_parameters() {
        let mut raw = RawPostgresOptions::new("db.example.invalid", "alice", "appdb");
        raw.application_name = "vtb-unit".to_string();
        raw.session_gucs
            .insert("TimeZone".to_string(), "UTC".to_string());
        let cfg = PostgresConfig::validate(raw).expect("valid config");

        let mut buf = BytesMut::new();
        encode_startup_frame(&cfg, &mut buf).expect("startup frame encodes");
        assert!(buf.len() > 8);
        let total_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(total_len, buf.len());
        let proto = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        assert_eq!(proto, 0x0003_0000);
        assert_eq!(buf[buf.len() - 1], 0);
        let body = String::from_utf8_lossy(&buf[8..]);
        assert!(body.contains("user\0alice\0"));
        assert!(body.contains("database\0appdb\0"));
        assert!(body.contains("client_encoding\0UTF8\0"));
        assert!(body.contains("application_name\0vtb-unit\0"));
        assert!(body.contains("TimeZone\0UTC\0"));
    }

    #[tokio::test]
    async fn read_message_reassembles_split_and_coalesced_frames() {
        let mut coalesced = Vec::new();
        coalesced.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]);
        let param_body = b"server_version\x0016.4\0";
        let param_len = (4 + param_body.len() as u32).to_be_bytes();
        coalesced.push(b'S');
        coalesced.extend_from_slice(&param_len);
        coalesced.extend_from_slice(param_body);
        coalesced.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);

        let chunks: Vec<Vec<u8>> = coalesced.iter().map(|b| vec![*b]).collect();
        let mut reader = ChunkedReader::new(chunks);
        let mut buf = BytesMut::new();

        let m1 = read_message(&mut reader, &mut buf, None)
            .await
            .expect("frame 1");
        assert!(matches!(m1, Message::AuthenticationOk));

        let m2 = read_message(&mut reader, &mut buf, None)
            .await
            .expect("frame 2");
        match m2 {
            Message::ParameterStatus(body) => {
                assert_eq!(body.name().unwrap(), "server_version");
                assert_eq!(body.value().unwrap(), "16.4");
            }
            _ => panic!("expected ParameterStatus"),
        }

        let m3 = read_message(&mut reader, &mut buf, None)
            .await
            .expect("frame 3");
        match m3 {
            Message::ReadyForQuery(body) => assert_eq!(body.status(), b'I'),
            _ => panic!("expected ReadyForQuery"),
        }
    }

    #[tokio::test]
    async fn read_message_rejects_oversized_and_undersized_headers_before_allocation() {
        let mut over = Vec::new();
        over.push(b'D');
        over.extend_from_slice(&((MAX_BACKEND_FRAME_LENGTH as u32) + 1).to_be_bytes());
        let mut reader = ChunkedReader::new(vec![over]);
        let mut buf = BytesMut::with_capacity(64);
        assert!(matches!(
            read_message(&mut reader, &mut buf, None).await,
            Err(PostgresError::Protocol(_))
        ));
        assert!(buf.capacity() <= 8192);

        let under = vec![b'Z', 0, 0, 0, 3];
        let mut reader2 = ChunkedReader::new(vec![under]);
        let mut buf2 = BytesMut::new();
        assert!(matches!(
            read_message(&mut reader2, &mut buf2, None).await,
            Err(PostgresError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn read_message_rejects_mid_frame_eof_and_unknown_tag() {
        let truncated = vec![b'R', 0, 0, 0, 8, 0, 0];
        let mut reader = ChunkedReader::new(vec![truncated]);
        let mut buf = BytesMut::new();
        assert!(matches!(
            read_message(&mut reader, &mut buf, None).await,
            Err(PostgresError::Transport(_))
        ));

        let bad_tag = vec![b'?', 0, 0, 0, 4];
        let mut reader2 = ChunkedReader::new(vec![bad_tag]);
        let mut buf2 = BytesMut::new();
        assert!(matches!(
            read_message(&mut reader2, &mut buf2, None).await,
            Err(PostgresError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn parse_error_fields_extracts_diagnostics_and_rejects_missing_sqlstate() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SERROR\0VFATAL\0C28P01\0Mpassword auth failed\0Ddetail-line\0\0");
        let mut frame = vec![b'E'];
        frame.extend_from_slice(&((4 + payload.len()) as u32).to_be_bytes());
        frame.extend_from_slice(&payload);

        let mut reader = ChunkedReader::new(vec![frame]);
        let mut buf = BytesMut::new();
        let msg = read_message(&mut reader, &mut buf, None)
            .await
            .expect("valid ErrorResponse frame");
        match msg {
            Message::ErrorResponse(body) => {
                let err = parse_error_fields(body.fields()).expect("valid ServerError");
                assert_eq!(err.sqlstate(), Some("28P01"));
                let rendered = err.to_string();
                assert!(rendered.contains("password auth failed"));
                assert!(!rendered.contains("detail-line"));
            }
            _ => panic!("expected ErrorResponse"),
        }
    }
}
