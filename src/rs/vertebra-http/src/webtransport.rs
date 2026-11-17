//! WebTransport over HTTP/3 session, bidirectional stream multiplexing, and
//! quarter-stream-ID datagram framing (`draft-ietf-webtrans-http3` / RFC 9297).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use h3::quic::{
    BidiStream as H3BidiStream, OpenStreams as H3OpenStreams, RecvStream as H3RecvStream,
    SendStream as H3SendStream,
};
use h3::stream::BufRecvStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

/// HTTP/3 frame type for a WebTransport bidirectional stream (`0x41`).
pub const WEBTRANSPORT_BI_STREAM_TYPE: u64 = 0x41;

/// Maximum queued incoming bidirectional streams per WebTransport session.
pub const DEFAULT_BIDI_CHANNEL_CAPACITY: usize = 64;

/// Maximum queued incoming datagrams per WebTransport session.
pub const DEFAULT_DATAGRAM_CHANNEL_CAPACITY: usize = 256;

const QUIC_VARINT_MAX: u64 = (1u64 << 62) - 1;

type WtBidiStream = h3_quinn::BidiStream<Bytes>;
type WtSendHalf = BufRecvStream<<WtBidiStream as H3BidiStream<Bytes>>::SendStream, Bytes>;
type WtRecvHalf = BufRecvStream<<WtBidiStream as H3BidiStream<Bytes>>::RecvStream, Bytes>;
pub(crate) type CloseCommand = (u32, String, oneshot::Sender<()>);

/// Errors returned by WebTransport session and stream operations.
#[derive(Debug, thiserror::Error)]
pub enum WebTransportError {
    #[error("WebTransport session {0} is closed")]
    SessionClosed(u64),
    #[error("QUIC stream error: {0}")]
    Stream(String),
    #[error("QUIC datagram error: {0}")]
    Datagram(String),
    #[error("invalid QUIC varint or WebTransport frame: {0}")]
    Protocol(String),
}

/// Encode a 62-bit QUIC variable-length integer (RFC 9000 §16).
pub fn encode_varint(value: u64, buf: &mut BytesMut) -> Result<(), WebTransportError> {
    if value < (1 << 6) {
        buf.put_u8(value as u8);
    } else if value < (1 << 14) {
        buf.put_u16((0b01u16 << 14) | value as u16);
    } else if value < (1 << 30) {
        buf.put_u32((0b10u32 << 30) | value as u32);
    } else if value <= QUIC_VARINT_MAX {
        buf.put_u64((0b11u64 << 62) | value);
    } else {
        return Err(WebTransportError::Protocol(format!(
            "QUIC varint overflow: {value}"
        )));
    }
    Ok(())
}

/// Decode a 62-bit QUIC variable-length integer (RFC 9000 §16).
pub fn decode_varint(buf: &mut impl Buf) -> Result<u64, WebTransportError> {
    if !buf.has_remaining() {
        return Err(WebTransportError::Protocol(
            "unexpected end of QUIC varint".to_string(),
        ));
    }
    let first = buf.get_u8();
    let len = 1usize << (first >> 6);
    if buf.remaining() < len - 1 {
        return Err(WebTransportError::Protocol(format!(
            "truncated QUIC varint: expected {len} bytes"
        )));
    }
    let mut acc = u64::from(first & 0x3f);
    for _ in 1..len {
        acc = (acc << 8) | u64::from(buf.get_u8());
    }
    Ok(acc)
}

/// Encode the `0x41 + SessionId` header prefix for a WebTransport bidirectional stream.
pub fn encode_bidi_stream_header(session_id: u64) -> Result<Bytes, WebTransportError> {
    let mut buf = BytesMut::with_capacity(16);
    encode_varint(WEBTRANSPORT_BI_STREAM_TYPE, &mut buf)?;
    encode_varint(session_id, &mut buf)?;
    Ok(buf.freeze())
}

/// Encode an HTTP/3 datagram frame prefixed with `quarter_stream_id = session_id >> 2` (RFC 9297).
pub fn encode_quarter_stream_datagram(
    session_id: u64,
    payload: &[u8],
) -> Result<Bytes, WebTransportError> {
    let quarter_stream_id = session_id >> 2;
    let mut buf = BytesMut::with_capacity(8 + payload.len());
    encode_varint(quarter_stream_id, &mut buf)?;
    buf.extend_from_slice(payload);
    Ok(buf.freeze())
}

/// Decode an HTTP/3 datagram frame into `(session_id, payload)` where `session_id = quarter_stream_id << 2`.
pub fn decode_quarter_stream_datagram(mut raw: Bytes) -> Result<(u64, Bytes), WebTransportError> {
    let quarter_stream_id = decode_varint(&mut raw)?;
    if quarter_stream_id > (QUIC_VARINT_MAX >> 2) {
        return Err(WebTransportError::Protocol(format!(
            "quarter_stream_id overflow: {quarter_stream_id}"
        )));
    }
    Ok((quarter_stream_id << 2, raw))
}

/// Bidirectional WebTransport stream backed by split buffered QUIC stream halves.
pub struct WebTransportStream {
    stream_id: u64,
    send: AsyncMutex<WtSendHalf>,
    recv: AsyncMutex<WtRecvHalf>,
}

impl WebTransportStream {
    pub(crate) fn from_buf_stream(stream: BufRecvStream<WtBidiStream, Bytes>) -> Self {
        let stream_id = H3SendStream::send_id(&stream).into_inner();
        let (send, recv) = H3BidiStream::split(stream);
        Self {
            stream_id,
            send: AsyncMutex::new(send),
            recv: AsyncMutex::new(recv),
        }
    }

    /// Return the underlying QUIC stream ID.
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Read up to `max_bytes` from the receive half of this stream. Returns empty `Bytes` on EOF.
    pub async fn recv(&self, max_bytes: usize) -> Result<Bytes, WebTransportError> {
        if max_bytes == 0 {
            return Ok(Bytes::new());
        }
        let mut buf = vec![0u8; max_bytes];
        let mut guard = self.recv.lock().await;
        let read = guard
            .read(&mut buf)
            .await
            .map_err(|e| WebTransportError::Stream(e.to_string()))?;
        buf.truncate(read);
        Ok(Bytes::from(buf))
    }

    /// Read the next QUIC chunk directly from the receive half. Returns `None` on EOF.
    pub async fn recv_chunk(&self) -> Result<Option<Bytes>, WebTransportError> {
        let mut guard = self.recv.lock().await;
        futures_util::future::poll_fn(|cx| guard.poll_data(cx))
            .await
            .map_err(|e| WebTransportError::Stream(e.to_string()))
    }

    /// Write all bytes in `data` onto the send half of this stream.
    pub async fn send(&self, data: &[u8]) -> Result<(), WebTransportError> {
        let mut guard = self.send.lock().await;
        guard
            .write_all(data)
            .await
            .map_err(|e| WebTransportError::Stream(e.to_string()))
    }

    /// Finish the send half of this stream (sending QUIC FIN).
    pub async fn finish(&self) -> Result<(), WebTransportError> {
        let mut guard = self.send.lock().await;
        guard
            .shutdown()
            .await
            .map_err(|e| WebTransportError::Stream(e.to_string()))
    }

    /// Abruptly reset the send half with `error_code`.
    pub async fn reset(&self, error_code: u64) {
        let mut guard = self.send.lock().await;
        H3SendStream::reset(&mut *guard, error_code);
    }

    /// Send `STOP_SENDING` on the receive half with `error_code`.
    pub async fn stop_sending(&self, error_code: u64) {
        let mut guard = self.recv.lock().await;
        H3RecvStream::stop_sending(&mut *guard, error_code);
    }
}

struct SessionEntry {
    bidi_tx: mpsc::Sender<WebTransportStream>,
    datagram_tx: mpsc::Sender<Bytes>,
    closed: Arc<AtomicBool>,
}

pub(crate) struct SessionRegistration {
    pub bidi_rx: mpsc::Receiver<WebTransportStream>,
    pub datagram_rx: mpsc::Receiver<Bytes>,
    pub closed: Arc<AtomicBool>,
}

/// Per-connection registry routing WebTransport bidirectional streams and datagrams
/// to active sessions by `session_id` while enforcing `max_sessions`.
pub(crate) struct WebTransportRegistry {
    max_sessions: u64,
    sessions: Mutex<HashMap<u64, SessionEntry>>,
}

impl WebTransportRegistry {
    pub(crate) fn new(max_sessions: u64) -> Self {
        Self {
            max_sessions,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn register(&self, session_id: u64) -> Option<SessionRegistration> {
        let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if (guard.len() as u64) >= self.max_sessions || guard.contains_key(&session_id) {
            return None;
        }
        let (bidi_tx, bidi_rx) = mpsc::channel(DEFAULT_BIDI_CHANNEL_CAPACITY);
        let (datagram_tx, datagram_rx) = mpsc::channel(DEFAULT_DATAGRAM_CHANNEL_CAPACITY);
        let closed = Arc::new(AtomicBool::new(false));
        guard.insert(
            session_id,
            SessionEntry {
                bidi_tx,
                datagram_tx,
                closed: Arc::clone(&closed),
            },
        );
        Some(SessionRegistration {
            bidi_rx,
            datagram_rx,
            closed,
        })
    }

    pub(crate) fn deregister(&self, session_id: u64) {
        let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = guard.remove(&session_id) {
            entry.closed.store(true, Ordering::Release);
        }
    }

    pub(crate) fn clear(&self) {
        let mut guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        for (_, entry) in guard.drain() {
            entry.closed.store(true, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize {
        let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        guard.len()
    }

    pub(crate) fn dispatch_bidi_stream(
        &self,
        session_id: u64,
        mut stream: BufRecvStream<WtBidiStream, Bytes>,
    ) -> bool {
        let tx = {
            let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            guard.get(&session_id).map(|entry| entry.bidi_tx.clone())
        };
        let Some(tx) = tx else {
            H3RecvStream::stop_sending(&mut stream, h3::error::Code::H3_REQUEST_REJECTED.value());
            H3SendStream::reset(&mut stream, h3::error::Code::H3_REQUEST_REJECTED.value());
            return false;
        };
        let wt_stream = WebTransportStream::from_buf_stream(stream);
        match tx.try_send(wt_stream) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(rejected))
            | Err(mpsc::error::TrySendError::Closed(rejected)) => {
                if let Ok(mut send) = rejected.send.try_lock() {
                    H3SendStream::reset(&mut *send, h3::error::Code::H3_REQUEST_REJECTED.value());
                }
                if let Ok(mut recv) = rejected.recv.try_lock() {
                    H3RecvStream::stop_sending(
                        &mut *recv,
                        h3::error::Code::H3_REQUEST_REJECTED.value(),
                    );
                }
                false
            }
        }
    }

    pub(crate) fn dispatch_datagram(&self, raw: Bytes) -> bool {
        let Ok((session_id, payload)) = decode_quarter_stream_datagram(raw) else {
            return false;
        };
        let tx = {
            let guard = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
            guard
                .get(&session_id)
                .map(|entry| entry.datagram_tx.clone())
        };
        let Some(tx) = tx else {
            return false;
        };
        tx.try_send(payload).is_ok()
    }
}

/// Active WebTransport session accepted via HTTP/3 extended `CONNECT`.
pub struct WebTransportSession {
    session_id: u64,
    path: String,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    bidi_rx: AsyncMutex<mpsc::Receiver<WebTransportStream>>,
    datagram_rx: AsyncMutex<mpsc::Receiver<Bytes>>,
    quic_conn: quinn::Connection,
    opener: h3_quinn::OpenStreams,
    registry: Arc<WebTransportRegistry>,
    closed: Arc<AtomicBool>,
    close_tx: AsyncMutex<Option<oneshot::Sender<CloseCommand>>>,
}

impl WebTransportSession {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_id: u64,
        path: String,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        registration: SessionRegistration,
        quic_conn: quinn::Connection,
        opener: h3_quinn::OpenStreams,
        registry: Arc<WebTransportRegistry>,
        close_tx: oneshot::Sender<CloseCommand>,
    ) -> Self {
        Self {
            session_id,
            path,
            headers,
            bidi_rx: AsyncMutex::new(registration.bidi_rx),
            datagram_rx: AsyncMutex::new(registration.datagram_rx),
            quic_conn,
            opener,
            registry,
            closed: registration.closed,
            close_tx: AsyncMutex::new(Some(close_tx)),
        }
    }

    /// Return the WebTransport session ID (the QUIC stream ID of the CONNECT request).
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Return the request path and query from the extended `CONNECT` request.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Return the request headers from the extended `CONNECT` request.
    pub fn headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.headers
    }

    /// Return `true` if this session has been closed or deregistered.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Accept the next client-initiated bidirectional stream for this session.
    pub async fn accept_bi(&self) -> Result<WebTransportStream, WebTransportError> {
        let mut guard = self.bidi_rx.lock().await;
        guard
            .recv()
            .await
            .ok_or(WebTransportError::SessionClosed(self.session_id))
    }

    /// Open a new server-initiated bidirectional stream prefixed with `0x41 + SessionId`.
    pub async fn open_bi(&self) -> Result<WebTransportStream, WebTransportError> {
        if self.is_closed() {
            return Err(WebTransportError::SessionClosed(self.session_id));
        }
        let mut opener = self.opener.clone();
        let stream = futures_util::future::poll_fn(|cx| opener.poll_open_bidi(cx))
            .await
            .map_err(|e| WebTransportError::Stream(e.to_string()))?;
        let wt_stream = WebTransportStream::from_buf_stream(BufRecvStream::new(stream));
        let header = encode_bidi_stream_header(self.session_id)?;
        wt_stream.send(&header).await?;
        Ok(wt_stream)
    }

    /// Receive the next incoming QUIC datagram routed to this session.
    pub async fn recv_datagram(&self) -> Result<Bytes, WebTransportError> {
        let mut guard = self.datagram_rx.lock().await;
        guard
            .recv()
            .await
            .ok_or(WebTransportError::SessionClosed(self.session_id))
    }

    /// Send a QUIC datagram prefixed with `quarter_stream_id = session_id >> 2`.
    pub fn send_datagram(&self, data: &[u8]) -> Result<(), WebTransportError> {
        if self.is_closed() {
            return Err(WebTransportError::SessionClosed(self.session_id));
        }
        let frame = encode_quarter_stream_datagram(self.session_id, data)?;
        self.quic_conn
            .send_datagram(frame)
            .map_err(|e| WebTransportError::Datagram(e.to_string()))
    }

    /// Close this WebTransport session and finish its underlying CONNECT stream.
    pub async fn close(&self, error_code: u32, reason: &str) -> Result<(), WebTransportError> {
        self.closed.store(true, Ordering::Release);
        self.registry.deregister(self.session_id);
        let tx = self.close_tx.lock().await.take();
        if let Some(tx) = tx {
            let (ack_tx, ack_rx) = oneshot::channel();
            if tx.send((error_code, reason.to_string(), ack_tx)).is_ok() {
                let _ = ack_rx.await;
            }
        }
        Ok(())
    }
}

impl Drop for WebTransportSession {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.registry.deregister(self.session_id);
        drop(self.close_tx.get_mut().take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip_all_widths_and_errors() {
        let values = [
            0u64,
            1,
            63,
            64,
            16_383,
            16_384,
            1_073_741_823,
            1_073_741_824,
            QUIC_VARINT_MAX,
        ];
        for &val in &values {
            let mut buf = BytesMut::new();
            encode_varint(val, &mut buf).unwrap();
            let mut reader = buf.freeze();
            let decoded = decode_varint(&mut reader).unwrap();
            assert_eq!(decoded, val);
            assert!(!reader.has_remaining());
        }

        let mut overflow_buf = BytesMut::new();
        assert!(encode_varint(QUIC_VARINT_MAX + 1, &mut overflow_buf).is_err());

        let mut empty = Bytes::new();
        assert!(decode_varint(&mut empty).is_err());

        let mut truncated = Bytes::from_static(&[0x40]);
        assert!(decode_varint(&mut truncated).is_err());
    }

    #[test]
    fn test_bidi_header_and_quarter_stream_datagram_framing() {
        let mut hdr = encode_bidi_stream_header(0).unwrap();
        assert_eq!(decode_varint(&mut hdr).unwrap(), 0x41);
        assert_eq!(decode_varint(&mut hdr).unwrap(), 0);

        let mut hdr4 = encode_bidi_stream_header(64).unwrap();
        assert_eq!(decode_varint(&mut hdr4).unwrap(), 0x41);
        assert_eq!(decode_varint(&mut hdr4).unwrap(), 64);

        let dgram = encode_quarter_stream_datagram(16, b"hello-wt").unwrap();
        let mut raw_check = dgram.clone();
        assert_eq!(decode_varint(&mut raw_check).unwrap(), 4);
        assert_eq!(raw_check.as_ref(), b"hello-wt");

        let (sid, payload) = decode_quarter_stream_datagram(dgram).unwrap();
        assert_eq!(sid, 16);
        assert_eq!(payload.as_ref(), b"hello-wt");
    }

    #[test]
    fn test_registry_capacity_deregister_and_bounded_datagram_drop() {
        let registry = WebTransportRegistry::new(1);
        let mut reg0 = registry.register(0).expect("first session must register");
        assert!(
            registry.register(4).is_none(),
            "second session must fail when max_sessions=1"
        );
        assert!(
            registry.register(0).is_none(),
            "duplicate session_id must fail"
        );
        assert_eq!(registry.active_count(), 1);

        for i in 0..(DEFAULT_DATAGRAM_CHANNEL_CAPACITY + 10) {
            let dgram = encode_quarter_stream_datagram(0, &[i as u8]).unwrap();
            let accepted = registry.dispatch_datagram(dgram);
            if i < DEFAULT_DATAGRAM_CHANNEL_CAPACITY {
                assert!(accepted);
            } else {
                assert!(!accepted, "datagram past capacity 256 must drop");
            }
        }
        assert!(reg0.datagram_rx.try_recv().is_ok());

        let unknown = encode_quarter_stream_datagram(12, b"stale").unwrap();
        assert!(!registry.dispatch_datagram(unknown));

        registry.deregister(0);
        assert_eq!(registry.active_count(), 0);
        assert!(reg0.closed.load(Ordering::Acquire));

        let stale = encode_quarter_stream_datagram(0, b"after-close").unwrap();
        assert!(!registry.dispatch_datagram(stale));
    }
}
