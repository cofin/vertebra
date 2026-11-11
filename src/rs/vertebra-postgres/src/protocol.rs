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

/// Backend message emitted on a PostgreSQL `replication=database` connection, extending standard
/// Protocol 3.0 frames with `CopyBothResponse` (`'W'`).
pub enum ReplicationBackendMessage {
    /// Logical/streaming replication `CopyBothResponse` (`'W'`) frame.
    CopyBothResponse {
        /// Overall stream format code (`0` required for logical replication).
        format: u8,
        /// Per-column format codes (must be empty for logical replication).
        column_formats: Vec<u16>,
    },
    /// Standard PostgreSQL Protocol 3.0 backend message decoded via `postgres-protocol`.
    Standard(Message),
}

impl std::fmt::Debug for ReplicationBackendMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CopyBothResponse {
                format,
                column_formats,
            } => f
                .debug_struct("CopyBothResponse")
                .field("format", format)
                .field("column_formats", column_formats)
                .finish(),
            Self::Standard(_) => f.write_str("Standard(..)"),
        }
    }
}

/// Read one bounded PostgreSQL backend frame `(tag, payload)` from `reader`.
///
/// Validates the 4-byte wire length (`4..=max_frame_bytes`, including the 4-byte length field
/// itself) from the 5-byte `[tag, len_be_u32]` header before reserving payload capacity, enforces
/// `deadline` with identical timeout semantics to [`read_message`], and leaves any coalesced
/// trailing frames untouched in `scratch`.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    scratch: &mut BytesMut,
    max_frame_bytes: usize,
    deadline: Option<Instant>,
) -> Result<(u8, BytesMut), PostgresError> {
    let mut raw_frame = read_bounded_raw_frame(reader, scratch, max_frame_bytes, deadline).await?;
    let header = raw_frame.split_to(5);
    Ok((header[0], raw_frame))
}

/// Read and decode one bounded replication backend message (`'W'` `CopyBothResponse` or standard
/// Protocol 3.0 backend frame) from `reader`.
///
/// Logical replication requires `CopyBothResponse` (`'W'`) with `format == 0` and zero columns;
/// truncated payloads, trailing bytes, nonzero formats, or nonzero column counts fail closed with
/// [`PostgresError::Protocol`]. Every non-`'W'` tag delegates directly to `postgres-protocol`.
pub async fn read_replication_message<R: AsyncRead + Unpin>(
    reader: &mut R,
    scratch: &mut BytesMut,
    max_frame_bytes: usize,
    deadline: Option<Instant>,
) -> Result<ReplicationBackendMessage, PostgresError> {
    let raw_frame = read_bounded_raw_frame(reader, scratch, max_frame_bytes, deadline).await?;
    if raw_frame[0] == b'W' {
        decode_copy_both_response(&raw_frame[5..])
    } else {
        parse_standard_frame(raw_frame).map(ReplicationBackendMessage::Standard)
    }
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
    let raw_frame = read_bounded_raw_frame(stream, buf, MAX_BACKEND_FRAME_LENGTH, deadline).await?;
    parse_standard_frame(raw_frame)
}

async fn read_bounded_raw_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    scratch: &mut BytesMut,
    max_frame_bytes: usize,
    deadline: Option<Instant>,
) -> Result<BytesMut, PostgresError> {
    if max_frame_bytes < 4 {
        return Err(PostgresError::Protocol(format!(
            "invalid max_frame_bytes {max_frame_bytes}; minimum PostgreSQL wire length is 4"
        )));
    }
    loop {
        if scratch.len() >= 5 {
            let raw_len =
                u32::from_be_bytes([scratch[1], scratch[2], scratch[3], scratch[4]]) as usize;
            if !(4..=max_frame_bytes).contains(&raw_len) {
                return Err(PostgresError::Protocol(format!(
                    "backend frame length {raw_len} is outside valid bounds 4..={max_frame_bytes}"
                )));
            }
            let total_frame_bytes = raw_len.checked_add(1).ok_or_else(|| {
                PostgresError::Protocol("backend frame length overflowed usize".to_string())
            })?;
            if scratch.len() >= total_frame_bytes {
                return Ok(scratch.split_to(total_frame_bytes));
            }
            let missing = total_frame_bytes - scratch.len();
            scratch.reserve(missing.min(64 * 1024));
        } else {
            scratch.reserve(4096);
        }

        let read_res = match deadline {
            Some(dl) => {
                if Instant::now() >= dl {
                    return Err(PostgresError::Transport(
                        "PostgreSQL frame read timed out".to_string(),
                    ));
                }
                tokio::time::timeout_at(dl, reader.read_buf(scratch))
                    .await
                    .map_err(|_| {
                        PostgresError::Transport("PostgreSQL frame read timed out".to_string())
                    })?
            }
            None => reader.read_buf(scratch).await,
        };

        let n =
            read_res.map_err(|e| PostgresError::Transport(format!("socket read error: {e}")))?;
        if n == 0 {
            return Err(PostgresError::Transport(
                "unexpected EOF from PostgreSQL backend".to_string(),
            ));
        }
    }
}

fn parse_standard_frame(mut raw_frame: BytesMut) -> Result<Message, PostgresError> {
    match Message::parse(&mut raw_frame) {
        Ok(Some(msg)) if raw_frame.is_empty() => Ok(msg),
        Ok(Some(_)) => Err(PostgresError::Protocol(
            "backend frame contained unconsumed trailing bytes after Message::parse".to_string(),
        )),
        Ok(None) => Err(PostgresError::Protocol(
            "backend frame header matched buffered length but Message::parse returned None"
                .to_string(),
        )),
        Err(err) => Err(PostgresError::Protocol(format!(
            "malformed backend frame: {err}"
        ))),
    }
}

fn decode_copy_both_response(payload: &[u8]) -> Result<ReplicationBackendMessage, PostgresError> {
    if payload.len() < 3 {
        return Err(PostgresError::Protocol(format!(
            "truncated CopyBothResponse ('W') payload of {} bytes (expected >= 3)",
            payload.len()
        )));
    }
    let format = payload[0];
    let raw_count = i16::from_be_bytes([payload[1], payload[2]]);
    if raw_count < 0 {
        return Err(PostgresError::Protocol(format!(
            "negative CopyBothResponse ('W') column count {raw_count}"
        )));
    }
    let count = raw_count as u16;
    let expected_len = 3_usize.saturating_add(usize::from(count).saturating_mul(2));
    if payload.len() != expected_len {
        return Err(PostgresError::Protocol(format!(
            "invalid CopyBothResponse ('W') payload length {} for {count} columns (expected {expected_len})",
            payload.len()
        )));
    }
    let mut column_formats = Vec::with_capacity(usize::from(count));
    for &chunk in payload[3..].as_chunks::<2>().0 {
        column_formats.push(u16::from_be_bytes(chunk));
    }
    if format != 0 || !column_formats.is_empty() {
        return Err(PostgresError::Protocol(format!(
            "logical replication CopyBothResponse ('W') requires format=0 and 0 columns, got format={format} columns={}",
            column_formats.len()
        )));
    }
    Ok(ReplicationBackendMessage::CopyBothResponse {
        format,
        column_formats,
    })
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
        payload
            .extend_from_slice(b"SERROR\0VFATAL\0C28P01\0Mpassword auth failed\0Ddetail-line\0\0");
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
