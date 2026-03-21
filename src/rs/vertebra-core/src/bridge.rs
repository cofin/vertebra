//! Arena-backed batch bridge between the HTTP server and Python workers.
//!
//! Requests are written into a shared arena + data slab. When a batch is
//! sealed, the bridge snapshots the arena+slab atomically and sends the
//! sealed snapshot through the channel. The arena is then reset for the next
//! batch without exposing post-seal writes to the worker.
//!
//! The response path uses a `tokio::sync::mpsc` channel for true async
//! notification — no spawn_blocking, no polling, no sleep.

use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::transport::{
    BatchSnapshot, BufferPool, DataSlab, Transport, TransportArena, TransportError, TransportNode,
};

/// Response data sent from Python workers back to the HTTP server.
#[derive(Debug)]
pub struct BridgeResponse {
    pub request_id: u64,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Request data to be written into the transport arena and slab.
pub struct WriteRequest<'a> {
    pub method_tag: u8,
    pub http_version_tag: u8,
    pub handler_id: u64,
    pub correlation_id: u64,
    pub path: &'a [u8],
    pub query: Option<&'a [u8]>,
    pub headers_raw: &'a [u8],
    pub body: &'a [u8],
}

/// Global batch bridge singleton.
static BRIDGE: OnceLock<BatchBridge> = OnceLock::new();

/// Arena-backed bidirectional bridge between the HTTP server and Python workers.
///
/// # Request path (zero-copy)
///
/// The dispatcher writes each request's variable-length data (path, query,
/// headers, body) into a [`DataSlab`] and pushes a fixed-size
/// [`TransportNode`] into the [`TransportArena`]. When the batch is sealed,
/// the bridge snapshots those buffers and sends the immutable snapshot
/// through the channel.
///
/// The worker calls `poll_batch()`, which receives that pre-sealed
/// [`BatchSnapshot`] as an `Arc` shared across all `VertebraRequest`
/// proxies in the batch.
///
/// # Response path
///
/// Python constructs owned `BridgeResponse` values and submits them through
/// a `tokio::sync::mpsc` channel. The HTTP server receives responses via
/// `recv_responses().await` — fully async, no spawn_blocking needed.
pub struct BatchBridge {
    // Request path: arena + slab + sealed snapshots.
    // Multi-consumer safe via crossbeam MPMC — N Python worker threads can
    // call poll_batch() concurrently; each batch is delivered to exactly one.
    arena: TransportArena,
    slab: DataSlab,
    write_lock: Mutex<()>,
    batch_tx: crossbeam_channel::Sender<Arc<BatchSnapshot>>,
    /// Unified abstraction for batch delivery (MPMC).
    pub transport: Transport,
    // Zero-copy buffer pool: pre-allocated buffer pairs recycled on snapshot drop
    buffer_pool: Arc<BufferPool>,
    // Response path: tokio::sync::mpsc for async recv on HTTP server side
    resp_tx: tokio::sync::mpsc::Sender<Vec<BridgeResponse>>,
    resp_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<BridgeResponse>>>,
}

/// Guard that serializes writes for a single batch.
pub struct BatchWriter<'a> {
    bridge: &'a BatchBridge,
    _guard: MutexGuard<'a, ()>,
}

impl BatchBridge {
    /// Initialize the global bridge. Call once at startup.
    ///
    /// - `arena_capacity`: max requests per batch cycle
    /// - `slab_capacity`: max bytes for variable-length data per batch
    /// - `channel_depth`: bounded channel capacity for sealed snapshots
    /// - `max_workers`: number of consumer threads (for buffer pool sizing)
    pub fn init(
        arena_capacity: usize,
        slab_capacity: usize,
        channel_depth: usize,
        max_workers: usize,
    ) -> Result<(), &'static str> {
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(channel_depth);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(channel_depth);
        // Pool needs: 1 active write buffer + channel_depth queued + max_workers
        // being dispatched concurrently. BufferPool::take() falls back to heap
        // allocation if the pool is exhausted, so this is a performance hint.
        let pool_size = channel_depth + max_workers.max(1);
        BRIDGE
            .set(BatchBridge {
                arena: TransportArena::new(arena_capacity),
                slab: DataSlab::new(slab_capacity),
                write_lock: Mutex::new(()),
                batch_tx,
                transport: Transport::new_in_process(batch_rx),
                buffer_pool: Arc::new(BufferPool::new(pool_size, slab_capacity, arena_capacity)),
                resp_tx,
                resp_rx: tokio::sync::Mutex::new(resp_rx),
            })
            .map_err(|_| "bridge already initialized")
    }

    /// Get the global bridge. Returns `None` if not initialized.
    pub fn get() -> Option<&'static BatchBridge> {
        BRIDGE.get()
    }

    /// Acquire exclusive access for writing one batch to the bridge.
    pub fn batch_writer(&self) -> BatchWriter<'_> {
        BatchWriter {
            bridge: self,
            _guard: self.write_lock.lock().unwrap(),
        }
    }

    /// Write a single request into the arena and slab.
    ///
    /// Called by the dispatcher for each request in a batch. The path, query,
    /// headers, and body bytes are written into the data slab; a compact
    /// [`TransportNode`] with slab offsets is pushed into the arena.
    pub fn push_request(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        self.batch_writer().push_request(req)
    }

    fn push_request_locked(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        let (path_off, path_len) = self.slab.write(req.path)?;
        let (query_off, query_len) = match req.query {
            Some(q) if !q.is_empty() => self.slab.write(q)?,
            _ => (0, 0),
        };
        let (hdr_off, hdr_len) = self.slab.write(req.headers_raw)?;
        let (body_off, body_len) = self.slab.write(req.body)?;

        self.arena.push(TransportNode {
            method_tag: req.method_tag,
            http_version_tag: req.http_version_tag,
            _pad: [0; 2],
            handler_id: req.handler_id,
            correlation_id: req.correlation_id,
            path_offset: path_off,
            path_len,
            query_offset: query_off,
            query_len,
            headers_offset: hdr_off,
            headers_len: hdr_len,
            body_offset: body_off,
            body_len,
        })?;

        Ok(())
    }

    /// Seal the current batch and notify the worker.
    ///
    /// Blocks if the previous batch hasn't been consumed (back-pressure).
    pub fn seal_batch(&self) -> Result<(), TransportError> {
        self.batch_writer().seal()
    }

    fn seal_batch_locked(&self) -> Result<(), TransportError> {
        let count = self.arena.len();
        if count == 0 {
            return Err(TransportError::EmptyBatch);
        }

        // Zero-copy seal: swap the active buffers with fresh ones from the pool.
        // take_and_replace is O(1) — a pointer swap, not a memcpy.
        let (fresh_slab, fresh_nodes) = self.buffer_pool.take();
        let slab_data = self.slab.take_and_replace(fresh_slab);
        let node_data = self.arena.take_and_replace(fresh_nodes);

        let snapshot = Arc::new(BatchSnapshot::new_pooled(
            slab_data,
            node_data,
            Arc::clone(&self.buffer_pool),
        ));
        self.batch_tx
            .send(snapshot)
            .map_err(|_| TransportError::Disconnected)
    }

    /// Poll for the next sealed batch snapshot.
    ///
    /// Safe to call concurrently from multiple worker threads — each batch
    /// is delivered to exactly one consumer (MPMC semantics via Transport).
    pub fn poll_batch(&self, timeout: Duration) -> Option<Arc<BatchSnapshot>> {
        self.transport.poll_batch(timeout)
    }

    /// Submit responses from a synchronous context (e.g., Python worker via FFI).
    ///
    /// Uses `blocking_send()` — must NOT be called from within a tokio runtime.
    pub fn submit_responses(
        &self,
        responses: Vec<BridgeResponse>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<BridgeResponse>>> {
        self.resp_tx.blocking_send(responses)
    }

    /// Submit responses from an async context.
    pub async fn submit_responses_async(
        &self,
        responses: Vec<BridgeResponse>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<BridgeResponse>>> {
        self.resp_tx.send(responses).await
    }

    /// Receive responses asynchronously (called by HTTP server response reader).
    ///
    /// Returns `None` when the channel is closed (bridge dropped).
    pub async fn recv_responses(&self) -> Option<Vec<BridgeResponse>> {
        let mut rx = self.resp_rx.lock().await;
        rx.recv().await
    }

    /// Non-blocking try_recv for responses.
    ///
    /// Safe to call from both sync and async contexts (uses `try_lock`).
    pub fn try_recv_responses(&self) -> Option<Vec<BridgeResponse>> {
        let mut rx = self.resp_rx.try_lock().ok()?;
        rx.try_recv().ok()
    }
}

impl BatchWriter<'_> {
    /// Write a single request into the batch while holding the batch lock.
    pub fn push_request(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        self.bridge.push_request_locked(req)
    }

    /// Seal the current batch and release the batch lock.
    pub fn seal(self) -> Result<(), TransportError> {
        self.bridge.seal_batch_locked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    fn make_bridge() -> BatchBridge {
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(4);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(4);
        BatchBridge {
            arena: TransportArena::new(64),
            slab: DataSlab::new(65536),
            write_lock: Mutex::new(()),
            batch_tx,
            batch_rx,
            buffer_pool: Arc::new(BufferPool::new(8, 65536, 64)),
            resp_tx,
            resp_rx: tokio::sync::Mutex::new(resp_rx),
        }
    }

    fn wr<'a>(
        method_tag: u8,
        handler_id: u64,
        correlation_id: u64,
        path: &'a [u8],
        query: Option<&'a [u8]>,
        headers_raw: &'a [u8],
        body: &'a [u8],
    ) -> WriteRequest<'a> {
        WriteRequest {
            method_tag,
            http_version_tag: 1,
            handler_id,
            correlation_id,
            path,
            query,
            headers_raw,
            body,
        }
    }

    #[test]
    fn push_request_and_seal() {
        let bridge = make_bridge();
        bridge
            .push_request(&wr(0, 42, 1, b"/hello", None, b"", b""))
            .unwrap();
        bridge
            .push_request(&wr(1, 43, 2, b"/echo", Some(b"q=1"), b"hdr", b"body"))
            .unwrap();
        bridge.seal_batch().unwrap();
    }

    #[test]
    fn seal_empty_batch_returns_error() {
        let bridge = make_bridge();
        let err = bridge.seal_batch().unwrap_err();
        assert!(matches!(err, TransportError::EmptyBatch));
    }

    #[test]
    fn push_seal_poll_roundtrip() {
        let bridge = make_bridge();
        bridge
            .push_request(&wr(0, 42, 1, b"/hello", None, b"", b""))
            .unwrap();
        bridge
            .push_request(&wr(1, 43, 2, b"/echo", Some(b"q=1"), b"hdr", b"body data"))
            .unwrap();
        bridge.seal_batch().unwrap();

        let snapshot = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(snapshot.len(), 2);

        assert_eq!(snapshot.path(0), "/hello");
        assert_eq!(snapshot.query(0), None);
        assert_eq!(snapshot.body(0), b"");
        assert_eq!(snapshot.node(0).handler_id, 42);
        assert_eq!(snapshot.node(0).correlation_id, 1);
        assert_eq!(snapshot.node(0).method_tag, 0);

        assert_eq!(snapshot.path(1), "/echo");
        assert_eq!(snapshot.query(1), Some("q=1"));
        assert_eq!(snapshot.body(1), b"body data");
        assert_eq!(snapshot.headers_raw(1), b"hdr");
        assert_eq!(snapshot.node(1).handler_id, 43);
    }

    #[test]
    fn poll_timeout_returns_none() {
        let bridge = make_bridge();
        let result = bridge.poll_batch(Duration::from_millis(10));
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn response_roundtrip() {
        let bridge = make_bridge();
        let responses = vec![BridgeResponse {
            request_id: 1,
            status: 200,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            body: b"hello world".to_vec(),
        }];
        bridge.resp_tx.send(responses).await.unwrap();

        let received = bridge.recv_responses().await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].request_id, 1);
        assert_eq!(received[0].status, 200);
        assert_eq!(received[0].body, b"hello world");
    }

    #[test]
    fn arena_resets_after_poll() {
        let bridge = make_bridge();
        bridge
            .push_request(&wr(0, 1, 1, b"/a", None, b"", b""))
            .unwrap();
        bridge.seal_batch().unwrap();
        let snap1 = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(snap1.len(), 1);

        bridge
            .push_request(&wr(0, 2, 2, b"/b", None, b"", b""))
            .unwrap();
        bridge
            .push_request(&wr(0, 3, 3, b"/c", None, b"", b""))
            .unwrap();
        bridge.seal_batch().unwrap();
        let snap2 = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(snap2.len(), 2);
        assert_eq!(snap2.path(0), "/b");
        assert_eq!(snap2.path(1), "/c");
    }

    #[test]
    fn multiple_batch_cycles() {
        let bridge = make_bridge();
        for cycle in 0..5u64 {
            for i in 0..3u64 {
                let path = format!("/cycle{}/req{}", cycle, i);
                bridge
                    .push_request(&wr(0, i, cycle * 3 + i, path.as_bytes(), None, b"", b""))
                    .unwrap();
            }
            bridge.seal_batch().unwrap();
            let snap = bridge.poll_batch(Duration::from_millis(100)).unwrap();
            assert_eq!(snap.len(), 3);
            for i in 0..3 {
                let expected = format!("/cycle{}/req{}", cycle, i);
                assert_eq!(snap.path(i), expected);
            }
        }
    }

    #[test]
    fn poll_batch_excludes_unsealed_next_batch() {
        let bridge = make_bridge();
        bridge
            .push_request(&wr(0, 1, 1, b"/sealed-a", None, b"", b""))
            .unwrap();
        bridge
            .push_request(&wr(0, 2, 2, b"/sealed-b", None, b"", b""))
            .unwrap();
        bridge.seal_batch().unwrap();

        bridge
            .push_request(&wr(0, 3, 3, b"/pending", None, b"", b""))
            .unwrap();

        let sealed = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(sealed.len(), 2);
        assert_eq!(sealed.path(0), "/sealed-a");
        assert_eq!(sealed.path(1), "/sealed-b");

        assert!(bridge.poll_batch(Duration::from_millis(10)).is_none());

        bridge.seal_batch().unwrap();
        let pending = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.path(0), "/pending");
    }

    #[test]
    fn batch_writer_serializes_competing_batches() {
        let bridge = Arc::new(make_bridge());
        let ready = Arc::new(Barrier::new(2));

        let bridge_bg = Arc::clone(&bridge);
        let ready_bg = Arc::clone(&ready);
        let join = thread::spawn(move || {
            ready_bg.wait();
            let writer = bridge_bg.batch_writer();
            writer
                .push_request(&wr(0, 2, 2, b"/second", None, b"", b""))
                .unwrap();
            writer.seal().unwrap();
        });

        let writer = bridge.batch_writer();
        writer
            .push_request(&wr(0, 1, 1, b"/first", None, b"", b""))
            .unwrap();
        ready.wait();
        writer.seal().unwrap();

        join.join().unwrap();

        let first = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        let second = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first.path(0), "/first");
        assert_eq!(second.len(), 1);
        assert_eq!(second.path(0), "/second");
    }

    #[test]
    fn push_with_all_fields() {
        let bridge = make_bridge();

        let mut headers = Vec::new();
        headers.push(4u8);
        headers.extend_from_slice(b"host");
        headers.extend_from_slice(&9u16.to_le_bytes());
        headers.extend_from_slice(b"localhost");

        bridge
            .push_request(&WriteRequest {
                method_tag: 1,
                http_version_tag: 2,
                handler_id: 999,
                correlation_id: 42,
                path: b"/api/items",
                query: Some(b"page=1&limit=10"),
                headers_raw: &headers,
                body: b"request body content",
            })
            .unwrap();
        bridge.seal_batch().unwrap();

        let snap = bridge.poll_batch(Duration::from_millis(100)).unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.path(0), "/api/items");
        assert_eq!(snap.query(0), Some("page=1&limit=10"));
        assert_eq!(snap.body(0), b"request body content");
        assert_eq!(snap.node(0).method_tag, 1);
        assert_eq!(snap.node(0).handler_id, 999);
        assert_eq!(snap.node(0).correlation_id, 42);

        let raw = snap.headers_raw(0);
        let decoded = crate::transport::decode_headers_raw(raw);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], ("host".to_string(), "localhost".to_string()));
    }

    #[test]
    fn multi_consumer_poll_batch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let bridge = Arc::new(make_bridge());
        let num_batches: u64 = 10;
        let num_consumers = 3;

        // Spawn N consumers first (they block on empty channel).
        let received = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..num_consumers)
            .map(|_| {
                let b = Arc::clone(&bridge);
                let r = Arc::clone(&received);
                thread::spawn(move || loop {
                    if b.poll_batch(Duration::from_millis(200)).is_some() {
                        r.fetch_add(1, Ordering::Relaxed);
                    } else {
                        break;
                    }
                })
            })
            .collect();

        // Seal N batches — consumers drain them concurrently so the
        // bounded channel (depth 4) never blocks the producer.
        for i in 0..num_batches {
            let path = format!("/req{i}");
            bridge
                .push_request(&wr(0, i, i, path.as_bytes(), None, b"", b""))
                .unwrap();
            bridge.seal_batch().unwrap();
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(Ordering::Relaxed), num_batches as usize);
    }
}
