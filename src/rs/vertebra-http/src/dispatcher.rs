//! Dispatcher: writes filled batches into the ring buffer for Python workers.
//!
//! In Chapter 5 the "ring buffer" is simulated by `tokio::sync::mpsc`.
//! Chapter 6 replaces this with the real shared-memory MPMC ring.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::collector::{Batch, PendingResponse};

/// Dispatcher state shared between the write task and the response router.
struct Inner {
    /// In-flight correlation ID -> reply channel.
    pending: Mutex<HashMap<u64, oneshot::Sender<PendingResponse>>>,
    /// Next monotonic correlation ID.
    next_id: std::sync::atomic::AtomicU64,
}

/// A batch serialised to bytes, ready to write to the ring buffer.
pub struct SerializedBatch {
    pub correlation_ids: Vec<u64>,
    pub payload: bytes::Bytes,
}

/// The dispatcher.
///
/// In Chapter 5 the "ring buffer" is a `tokio::sync::mpsc`. Chapter 6 replaces
/// this with the real shared-memory MPMC ring from `vertebra-core`.
pub struct Dispatcher {
    inner: Arc<Inner>,
    /// Channel to the ring-writer task. Bounded to apply back-pressure.
    ring_tx: tokio::sync::mpsc::Sender<SerializedBatch>,
}

impl Dispatcher {
    /// Create a dispatcher and return both the dispatcher and the ring-reader.
    pub fn new(ring_capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<SerializedBatch>) {
        let (ring_tx, ring_rx) = tokio::sync::mpsc::channel(ring_capacity);
        let dispatcher = Self {
            inner: Arc::new(Inner {
                pending: Mutex::new(HashMap::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
            }),
            ring_tx,
        };
        (dispatcher, ring_rx)
    }

    /// Dispatch a batch to the ring buffer.
    ///
    /// Assigns a correlation ID to each request, serialises the batch, and
    /// sends it to the ring-writer task.
    pub async fn dispatch(&self, batch: Batch) {
        let serialized = self.serialize_batch(batch);

        if let Err(e) = self.ring_tx.try_send(serialized) {
            error!(error = %e, "ring buffer full, dropping batch");
        }
    }

    /// Synchronous serialization — keeps the MutexGuard out of the async future.
    fn serialize_batch(&self, batch: Batch) -> SerializedBatch {
        use std::sync::atomic::Ordering;

        let mut ids = Vec::with_capacity(batch.requests.len());
        let mut pending = self.inner.pending.lock();

        let mut payload_parts: Vec<bytes::Bytes> = Vec::new();

        for req in batch.requests {
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            ids.push(id);

            pending.insert(id, req.reply);

            let mut header = [0u8; 16];
            header[0..8].copy_from_slice(&id.to_le_bytes());
            header[8..12].copy_from_slice(&0u32.to_le_bytes());
            let body_len = req.body.len() as u32;
            header[12..16].copy_from_slice(&body_len.to_le_bytes());

            payload_parts.push(bytes::Bytes::copy_from_slice(&header));
            payload_parts.push(req.body);
        }

        drop(pending);

        let payload = payload_parts
            .into_iter()
            .fold(bytes::BytesMut::new(), |mut acc, b| {
                acc.extend_from_slice(&b);
                acc
            })
            .freeze();

        SerializedBatch {
            correlation_ids: ids,
            payload,
        }
    }

    /// Complete a request with a response from a Python worker.
    pub fn complete(&self, correlation_id: u64, response: PendingResponse) {
        let mut pending = self.inner.pending.lock();
        if let Some(tx) = pending.remove(&correlation_id) {
            let _ = tx.send(response);
        } else {
            debug!(id = correlation_id, "no pending request for correlation ID");
        }
    }
}
