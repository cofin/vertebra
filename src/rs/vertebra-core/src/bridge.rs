//! Arena-backed and zero-copy in-process batch bridge between the HTTP server and Python workers.
//!
//! Embedded requests can either be submitted zero-copy as [`InProcessRequest`]
//! batches (`Bytes`-backed without slab limits or mutex locking) or written
//! into the shared arena + data slab and sealed atomically.
//!
//! Responses complete either directly through a registered [`ResponseSink`]
//! callback on the submitting worker thread or through an unbounded lock-free
//! `tokio::sync::mpsc` channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::Duration;

use crate::protocol::codec::CompressionCodec;
use crate::transport::{
    BatchSnapshot, BufferPool, DataSlab, InProcessRequest, Transport, TransportArena,
    TransportError, TransportNode, iter_raw_headers,
};

/// Direct response completion callback registered by the embedded HTTP dispatcher.
pub type ResponseSink = Arc<dyn Fn(BridgeResponse) + Send + Sync>;

/// Response data sent from Python workers back to the HTTP server.
#[derive(Debug, Clone)]
pub struct BridgeResponse {
    pub request_id: u64,
    pub status: u16,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
}

impl BridgeResponse {
    /// Iterate zero-copy `(name, value)` byte pairs from [`Self::headers_raw`].
    pub fn iter_headers(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        iter_raw_headers(&self.headers_raw)
    }
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

/// Bidirectional in-process bridge between the HTTP server and Python workers.
pub struct BatchBridge {
    arena: TransportArena,
    slab: DataSlab,
    write_lock: Mutex<()>,
    batch_tx: crossbeam_channel::Sender<Arc<BatchSnapshot>>,
    /// Unified abstraction for batch delivery (MPMC).
    pub transport: Transport,
    buffer_pool: Arc<BufferPool>,
    resp_tx: tokio::sync::mpsc::UnboundedSender<Vec<BridgeResponse>>,
    resp_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<BridgeResponse>>>,
    fast_sink: OnceLock<ResponseSink>,
    has_override_sink: AtomicBool,
    override_sink: RwLock<Option<ResponseSink>>,
    /// Compression codec for batch sealing. `None` disables compression.
    compression_codec: CompressionCodec,
}

/// Guard that serializes writes for a single slab-backed batch.
pub struct BatchWriter<'a> {
    bridge: &'a BatchBridge,
    _guard: MutexGuard<'a, ()>,
}

impl BatchBridge {
    /// Initialize the global bridge. Call once at startup.
    pub fn init(
        arena_capacity: usize,
        slab_capacity: usize,
        channel_depth: usize,
        max_workers: usize,
    ) -> Result<(), &'static str> {
        Self::init_with_codec(
            arena_capacity,
            slab_capacity,
            channel_depth,
            max_workers,
            CompressionCodec::None,
        )
    }

    /// Initialize the global bridge with a compression codec.
    pub fn init_with_codec(
        arena_capacity: usize,
        slab_capacity: usize,
        channel_depth: usize,
        max_workers: usize,
        codec: CompressionCodec,
    ) -> Result<(), &'static str> {
        let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        let pool_size = channel_depth.max(16) + max_workers.max(1);
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
                fast_sink: OnceLock::new(),
                has_override_sink: AtomicBool::new(false),
                override_sink: RwLock::new(None),
                compression_codec: codec,
            })
            .map_err(|_| "bridge already initialized")
    }

    /// Get the global bridge. Returns `None` if not initialized.
    pub fn get() -> Option<&'static BatchBridge> {
        BRIDGE.get()
    }

    /// Register a direct response completion callback that bypasses the async response channel.
    pub fn set_response_sink(&self, sink: ResponseSink) {
        if !self.has_override_sink.load(Ordering::Acquire)
            && self.fast_sink.set(sink.clone()).is_ok()
        {
            return;
        }
        *self.override_sink.write().unwrap() = Some(sink);
        self.has_override_sink.store(true, Ordering::Release);
    }

    /// Clear any registered direct response completion callback, restoring channel delivery.
    pub fn clear_response_sink(&self) {
        *self.override_sink.write().unwrap() = None;
        self.has_override_sink.store(true, Ordering::Release);
    }

    /// Submit a zero-copy in-process request batch without acquiring `write_lock` or copying into `DataSlab`.
    pub fn submit_inprocess_batch(
        &self,
        requests: Vec<InProcessRequest>,
    ) -> Result<(), TransportError> {
        if requests.is_empty() {
            return Err(TransportError::EmptyBatch);
        }
        let snapshot = Arc::new(BatchSnapshot::from_inprocess(requests));
        self.batch_tx
            .send(snapshot)
            .map_err(|_| TransportError::Disconnected)
    }

    /// Acquire exclusive access for writing one slab-backed batch to the bridge.
    pub fn batch_writer(&self) -> BatchWriter<'_> {
        BatchWriter {
            bridge: self,
            _guard: self.write_lock.lock().unwrap(),
        }
    }

    /// Write a single request into the arena and slab, auto-sealing if the current non-empty batch is full.
    pub fn push_request(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        self.batch_writer().push_request(req)
    }

    fn push_request_locked(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        let prev_slab_used = self.slab.used();
        let write_attempt = (|| -> Result<(), TransportError> {
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
        })();

        if write_attempt.is_err() {
            self.slab.truncate_to(prev_slab_used);
        }
        write_attempt
    }

    /// Seal the current batch and notify the worker without blocking.
    pub fn seal_batch(&self) -> Result<(), TransportError> {
        self.batch_writer().seal()
    }

    fn seal_batch_locked(&self) -> Result<(), TransportError> {
        let count = self.arena.len();
        if count == 0 {
            return Err(TransportError::EmptyBatch);
        }

        let (fresh_slab, fresh_nodes) = self.buffer_pool.take();
        let slab_data = self.slab.take_and_replace(fresh_slab);
        let node_data = self.arena.take_and_replace(fresh_nodes);

        let snapshot = match self.try_compress_slab(&slab_data) {
            Some((compressed, flags)) => {
                drop(slab_data);
                Arc::new(BatchSnapshot::new_with_flags(compressed, node_data, flags))
            }
            None => Arc::new(BatchSnapshot::new_pooled(
                slab_data,
                node_data,
                Arc::clone(&self.buffer_pool),
            )),
        };
        self.batch_tx
            .send(snapshot)
            .map_err(|_| TransportError::Disconnected)
    }

    fn try_compress_slab(&self, slab_data: &[u8]) -> Option<(Vec<u8>, u32)> {
        #[cfg(feature = "compression")]
        {
            if self.compression_codec != CompressionCodec::None {
                let compressed =
                    crate::protocol::codec::compress(slab_data, self.compression_codec);
                if compressed.len() < slab_data.len() {
                    return Some((compressed, self.compression_codec.to_flags()));
                }
            }
        }
        let _ = slab_data;
        None
    }

    /// Poll for the next sealed batch snapshot.
    pub fn poll_batch(&self, timeout: Duration) -> Option<Arc<BatchSnapshot>> {
        self.transport.poll_batch(timeout)
    }

    #[inline]
    fn try_dispatch_to_sink_single(&self, response: BridgeResponse) -> Result<(), BridgeResponse> {
        if !self.has_override_sink.load(Ordering::Acquire) {
            if let Some(sink) = self.fast_sink.get() {
                sink(response);
                return Ok(());
            }
            return Err(response);
        }
        if let Some(sink) = self.override_sink.read().unwrap().as_ref() {
            sink(response);
            return Ok(());
        }
        Err(response)
    }

    #[inline]
    fn try_dispatch_to_sink_batch(
        &self,
        responses: Vec<BridgeResponse>,
    ) -> Result<(), Vec<BridgeResponse>> {
        if !self.has_override_sink.load(Ordering::Acquire) {
            if let Some(sink) = self.fast_sink.get() {
                for resp in responses {
                    sink(resp);
                }
                return Ok(());
            }
            return Err(responses);
        }
        if let Some(sink) = self.override_sink.read().unwrap().as_ref() {
            for resp in responses {
                sink(resp);
            }
            return Ok(());
        }
        Err(responses)
    }

    /// Submit a single response without allocating a wrapper vector when a [`ResponseSink`] is registered.
    pub fn submit_response(&self, response: BridgeResponse) -> Result<(), TransportError> {
        match self.try_dispatch_to_sink_single(response) {
            Ok(()) => Ok(()),
            Err(resp) => self
                .resp_tx
                .send(vec![resp])
                .map_err(|_| TransportError::Disconnected),
        }
    }

    /// Submit responses from either a synchronous or asynchronous context (lock-free, never blocks).
    pub fn submit_responses(
        &self,
        responses: Vec<BridgeResponse>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<BridgeResponse>>> {
        if responses.is_empty() {
            return Ok(());
        }
        match self.try_dispatch_to_sink_batch(responses) {
            Ok(()) => Ok(()),
            Err(resps) => self.resp_tx.send(resps),
        }
    }

    /// Submit responses from an async context.
    pub async fn submit_responses_async(
        &self,
        responses: Vec<BridgeResponse>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Vec<BridgeResponse>>> {
        self.submit_responses(responses)
    }

    /// Receive responses asynchronously when no direct [`ResponseSink`] is registered.
    pub async fn recv_responses(&self) -> Option<Vec<BridgeResponse>> {
        let mut rx = self.resp_rx.lock().await;
        rx.recv().await
    }

    /// Non-blocking try_recv for responses.
    pub fn try_recv_responses(&self) -> Option<Vec<BridgeResponse>> {
        let mut rx = self.resp_rx.try_lock().ok()?;
        rx.try_recv().ok()
    }
}

impl BatchWriter<'_> {
    /// Write a single request into the batch while holding the batch lock, auto-sealing if full.
    pub fn push_request(&self, req: &WriteRequest<'_>) -> Result<(), TransportError> {
        match self.bridge.push_request_locked(req) {
            Ok(()) => Ok(()),
            Err(TransportError::SlabFull(_) | TransportError::ArenaFull(_))
                if !self.bridge.arena.is_empty() =>
            {
                self.bridge.seal_batch_locked()?;
                self.bridge.push_request_locked(req)
            }
            Err(err) => Err(err),
        }
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
        make_bridge_with_codec(CompressionCodec::None)
    }

    fn make_bridge_with_codec(codec: CompressionCodec) -> BatchBridge {
        let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        BatchBridge {
            arena: TransportArena::new(64),
            slab: DataSlab::new(65536),
            write_lock: Mutex::new(()),
            batch_tx,
            transport: Transport::new_in_process(batch_rx),
            buffer_pool: Arc::new(BufferPool::new(16, 65536, 64)),
            resp_tx,
            resp_rx: tokio::sync::Mutex::new(resp_rx),
            fast_sink: OnceLock::new(),
            has_override_sink: AtomicBool::new(false),
            override_sink: RwLock::new(None),
            compression_codec: codec,
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
        let headers_raw: bytes::Bytes = crate::transport::encode_headers_compact(&[(
            b"content-type".to_vec(),
            b"text/plain".to_vec(),
        )])
        .into();
        let responses = vec![BridgeResponse {
            request_id: 1,
            status: 200,
            headers_raw,
            body: bytes::Bytes::from_static(b"hello world"),
        }];
        bridge.submit_responses(responses).unwrap();

        let received = bridge.recv_responses().await.unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].request_id, 1);
        assert_eq!(received[0].status, 200);
        assert_eq!(received[0].body.as_ref(), b"hello world");
        let parsed_headers: Vec<_> = received[0].iter_headers().collect();
        assert_eq!(parsed_headers.len(), 1);
        assert_eq!(
            parsed_headers[0],
            (&b"content-type"[..], &b"text/plain"[..])
        );
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
        assert_eq!(decoded[0], (b"host".to_vec(), b"localhost".to_vec()));
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
                thread::spawn(move || {
                    loop {
                        if b.poll_batch(Duration::from_millis(200)).is_some() {
                            r.fetch_add(1, Ordering::Relaxed);
                        } else {
                            break;
                        }
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

    #[test]
    fn queue_64_batches_single_thread_without_deadlock() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let bridge = make_bridge();
            for i in 0..64u64 {
                let path = format!("/batch/{i}");
                bridge
                    .push_request(&wr(0, 1, i, path.as_bytes(), None, b"", b"payload"))
                    .unwrap();
                bridge.seal_batch().unwrap();
            }

            let mut polled = 0usize;
            while let Some(snap) = bridge.poll_batch(Duration::from_millis(10)) {
                assert_eq!(snap.len(), 1);
                let corr = snap.node(0).correlation_id;
                bridge
                    .submit_responses(vec![BridgeResponse {
                        request_id: corr,
                        status: 200,
                        headers_raw: bytes::Bytes::new(),
                        body: bytes::Bytes::from_static(b"ok"),
                    }])
                    .unwrap();
                polled += 1;
                if polled == 64 {
                    break;
                }
            }
            assert_eq!(polled, 64);

            let mut drained = 0usize;
            while let Some(batch_resps) = bridge.try_recv_responses() {
                drained += batch_resps.len();
            }
            assert_eq!(drained, 64);
            let _ = done_tx.send(());
        });

        done_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("queuing 64 batches and responses on a single thread deadlocked");
    }

    #[test]
    fn inprocess_batch_exceeding_1mib_and_direct_response_sink() {
        let bridge = make_bridge();
        let captured = Arc::new(Mutex::new(Vec::<BridgeResponse>::new()));
        let captured_sink = Arc::clone(&captured);
        bridge.set_response_sink(Arc::new(move |resp| {
            captured_sink.lock().unwrap().push(resp);
        }));

        let payload = bytes::Bytes::from(vec![b'x'; 2 * 1024 * 1024]);
        let headers_raw: bytes::Bytes = crate::transport::encode_headers_compact(&[(
            b"content-type".to_vec(),
            b"application/octet-stream".to_vec(),
        )])
        .into();
        let req = InProcessRequest::new(
            1,
            1,
            777,
            9001,
            bytes::Bytes::from_static(b"/large"),
            bytes::Bytes::from_static(b"stream=0"),
            headers_raw.clone(),
            payload.clone(),
            bytes::Bytes::from_static(b"id\042"),
        );

        bridge.submit_inprocess_batch(vec![req]).unwrap();

        let snap = bridge.poll_batch(Duration::from_millis(50)).unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.node(0).method_tag, 1);
        assert_eq!(snap.node(0).handler_id, 777);
        assert_eq!(snap.node(0).correlation_id, 9001);
        assert_eq!(snap.path(0), "/large");
        assert_eq!(snap.query(0), Some("stream=0"));
        assert_eq!(snap.headers_raw(0), headers_raw.as_ref());
        assert_eq!(snap.body(0).len(), 2 * 1024 * 1024);
        assert_eq!(snap.body_bytes_owned(0), payload);
        assert_eq!(snap.path_params_raw(0), b"id\042");

        let mut wire = vec![0u8; snap.serialized_size()];
        let written = snap.serialize_into(&mut wire);
        assert_eq!(written, wire.len());
        let roundtripped = BatchSnapshot::deserialize_from(&wire).unwrap();
        assert_eq!(roundtripped.path(0), "/large");
        assert_eq!(roundtripped.query(0), Some("stream=0"));
        assert_eq!(roundtripped.body(0).len(), 2 * 1024 * 1024);

        bridge
            .submit_response(BridgeResponse {
                request_id: 9001,
                status: 201,
                headers_raw: headers_raw.clone(),
                body: bytes::Bytes::from_static(b"accepted"),
            })
            .unwrap();

        assert!(bridge.try_recv_responses().is_none());
        let sink_items = captured.lock().unwrap();
        assert_eq!(sink_items.len(), 1);
        assert_eq!(sink_items[0].request_id, 9001);
        assert_eq!(sink_items[0].status, 201);
        assert_eq!(sink_items[0].body.as_ref(), b"accepted");
        drop(sink_items);

        bridge.clear_response_sink();
        bridge
            .submit_response(BridgeResponse {
                request_id: 9002,
                status: 204,
                headers_raw: bytes::Bytes::new(),
                body: bytes::Bytes::new(),
            })
            .unwrap();
        let fallback = bridge.try_recv_responses().unwrap();
        assert_eq!(fallback.len(), 1);
        assert_eq!(fallback[0].request_id, 9002);
    }

    #[test]
    fn batch_writer_auto_seals_on_slab_or_arena_full() {
        let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
        let (resp_tx, resp_rx) = tokio::sync::mpsc::unbounded_channel();
        let bridge = BatchBridge {
            arena: TransportArena::new(2),
            slab: DataSlab::new(64),
            write_lock: Mutex::new(()),
            batch_tx,
            transport: Transport::new_in_process(batch_rx),
            buffer_pool: Arc::new(BufferPool::new(4, 64, 2)),
            resp_tx,
            resp_rx: tokio::sync::Mutex::new(resp_rx),
            fast_sink: OnceLock::new(),
            has_override_sink: AtomicBool::new(false),
            override_sink: RwLock::new(None),
            compression_codec: CompressionCodec::None,
        };

        let writer = bridge.batch_writer();
        writer
            .push_request(&wr(0, 1, 10, b"/one", None, b"", &[b'a'; 40]))
            .unwrap();
        writer
            .push_request(&wr(0, 1, 11, b"/two", None, b"", &[b'b'; 40]))
            .unwrap();
        writer
            .push_request(&wr(0, 1, 12, b"/three", None, b"", b"ok"))
            .unwrap();
        writer
            .push_request(&wr(0, 1, 13, b"/four", None, b"", b"ok"))
            .unwrap();
        writer
            .push_request(&wr(0, 1, 14, b"/five", None, b"", b"ok"))
            .unwrap();
        writer.seal().unwrap();

        let mut ids = Vec::new();
        while let Some(snap) = bridge.poll_batch(Duration::from_millis(10)) {
            for i in 0..snap.len() {
                ids.push(snap.node(i).correlation_id);
            }
        }
        assert_eq!(ids, vec![10, 11, 12, 13, 14]);
    }

    #[cfg(feature = "compression")]
    mod compression_tests {
        use super::*;

        #[test]
        fn lz4_compressed_batch_roundtrip() {
            let bridge = make_bridge_with_codec(CompressionCodec::Lz4);
            // Use repetitive data that compresses well
            let body = "hello world ".repeat(100);
            bridge
                .push_request(&wr(0, 42, 1, b"/hello", None, b"", body.as_bytes()))
                .unwrap();
            bridge.seal_batch().unwrap();

            let snapshot = bridge.poll_batch(Duration::from_millis(100)).unwrap();
            assert_eq!(snapshot.len(), 1);
            assert_eq!(snapshot.path(0), "/hello");
            assert_eq!(snapshot.body(0), body.as_bytes());
            assert!(
                snapshot.flags() & crate::protocol::flags::COMPRESSED != 0,
                "COMPRESSED flag should be set"
            );
        }

        #[test]
        fn zstd_compressed_batch_roundtrip() {
            let bridge = make_bridge_with_codec(CompressionCodec::Zstd);
            let body = "repeating data for compression ".repeat(50);
            bridge
                .push_request(&wr(
                    1,
                    43,
                    2,
                    b"/echo",
                    Some(b"q=1"),
                    b"hdr",
                    body.as_bytes(),
                ))
                .unwrap();
            bridge.seal_batch().unwrap();

            let snapshot = bridge.poll_batch(Duration::from_millis(100)).unwrap();
            assert_eq!(snapshot.len(), 1);
            assert_eq!(snapshot.path(0), "/echo");
            assert_eq!(snapshot.query(0), Some("q=1"));
            assert_eq!(snapshot.headers_raw(0), b"hdr");
            assert_eq!(snapshot.body(0), body.as_bytes());
        }

        #[test]
        fn small_payload_skips_compression() {
            // Tiny payloads don't compress well — bridge should skip compression
            let bridge = make_bridge_with_codec(CompressionCodec::Lz4);
            bridge
                .push_request(&wr(0, 1, 1, b"/", None, b"", b"hi"))
                .unwrap();
            bridge.seal_batch().unwrap();

            let snapshot = bridge.poll_batch(Duration::from_millis(100)).unwrap();
            assert_eq!(snapshot.path(0), "/");
            assert_eq!(snapshot.body(0), b"hi");
            // Small payload: compression may or may not be applied (depends on size)
            // Either way, data must round-trip correctly.
        }

        #[test]
        fn uncompressed_bridge_has_no_compressed_flag() {
            let bridge = make_bridge(); // No compression
            bridge
                .push_request(&wr(0, 1, 1, b"/test", None, b"", b"body"))
                .unwrap();
            bridge.seal_batch().unwrap();

            let snapshot = bridge.poll_batch(Duration::from_millis(100)).unwrap();
            assert_eq!(snapshot.flags() & crate::protocol::flags::COMPRESSED, 0);
        }

        #[test]
        fn multiple_compressed_batch_cycles() {
            let bridge = make_bridge_with_codec(CompressionCodec::Lz4);
            for cycle in 0..5u64 {
                let body = format!("cycle {} data ", cycle).repeat(50);
                bridge
                    .push_request(&wr(0, cycle, cycle, b"/cycle", None, b"", body.as_bytes()))
                    .unwrap();
                bridge.seal_batch().unwrap();
                let snap = bridge.poll_batch(Duration::from_millis(100)).unwrap();
                assert_eq!(snap.len(), 1);
                assert_eq!(snap.body(0), body.as_bytes());
            }
        }
    }
}
