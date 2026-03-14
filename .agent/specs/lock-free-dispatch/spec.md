# Spec: lock-free-dispatch

**Flow ID:** `lock-free-dispatch`
**Epic:** `vertebra-k4k.3`
**Status:** Planning
**Parent PRD:** async-ipc-pipelining

## Summary

Remove `Arc<Mutex<Worker>>` serialization that forces one request per worker. Replace with lock-free dispatch using atomic counters and least-pending worker selection.

## Code Analysis Summary

### Current State

**`src/rs/vertebra-server/src/dispatcher.rs`:**
```rust
pub struct RoundRobinDispatcher {
    workers: Vec<Arc<Mutex<Worker>>>,  // MUTEX = SERIALIZATION
    next: AtomicUsize,
}

impl RoundRobinDispatcher {
    pub fn next(&self) -> Arc<Mutex<Worker>> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        Arc::clone(&self.workers[idx])
    }
}
```

**`src/rs/vertebra-server/src/bridge.rs`:**
```rust
pub async fn dispatch(..., worker: Arc<Mutex<Worker>>) -> ... {
    let worker = worker.lock().await;  // BLOCKS OTHER REQUESTS
    // ... entire request lifetime holds lock ...
}
```

### Problem

The `lock().await` means only ONE request can be processed per worker at any time. Even with async eventfd (Chapter 1) and correlation (Chapter 2), requests still serialize through this mutex.

### Target State

No mutex, multiple concurrent requests per worker:
```rust
pub struct Dispatcher {
    workers: Vec<Arc<WorkerConnection>>,  // No mutex!
    // ...
}

pub async fn dispatch(..., worker: Arc<WorkerConnection>) -> ... {
    // No lock! Multiple requests can be in-flight
    let req_id = worker.next_request_id();
    let (tx, rx) = oneshot::channel();
    worker.pending.insert(req_id, tx);
    worker.req_tx.send(&message, 0)?;  // Concurrent sends OK
    // ...
}
```

## Requirements

### R1: Remove Mutex from Dispatcher
Replace `Arc<Mutex<Worker>>` with `Arc<WorkerConnection>`:
```rust
pub struct Dispatcher {
    workers: Vec<Arc<WorkerConnection>>,
}
```

### R2: Atomic Pending Counters
Track in-flight requests per worker for load balancing:
```rust
pub struct WorkerConnection {
    pending_count: AtomicU32,
    // ...
}
```

### R3: Least-Pending Selection
Route new requests to worker with fewest in-flight:
```rust
impl Dispatcher {
    pub fn select_worker(&self) -> Arc<WorkerConnection> {
        self.workers
            .iter()
            .min_by_key(|w| w.pending_count.load(Ordering::Relaxed))
            .map(Arc::clone)
            .unwrap()
    }
}
```

### R4: Thread-Safe Ring Buffer Send
Ensure `SpscSender.send()` is safe for concurrent calls.

**Analysis:** SPSC = Single Producer Single Consumer. By definition, SPSC is NOT thread-safe for multiple producers.

**Solution Options:**
1. Use MPSC ring buffer instead (Multiple Producer, Single Consumer)
2. Use mutex ONLY around the send operation (much smaller critical section)
3. Use atomic ticket-based serialization for sends

Recommend Option 2 for minimal changes:
```rust
pub struct WorkerConnection {
    req_tx: Mutex<SpscSender>,  // Small mutex, just for send
    // ...
}
```

### R5: Backpressure Handling
When ring buffer is full, handle gracefully:
```rust
// In dispatch:
match worker.req_tx.lock().await.try_send(&message, 0) {
    Ok(()) => { /* sent */ }
    Err(ChannelError::Full) => {
        // Option A: Wait and retry
        // Option B: Return 503 Service Unavailable
        // Option C: Route to different worker
    }
}
```

### R6: Request Cleanup on Timeout/Error
Remove pending entry and decrement counter on failure:
```rust
let result = tokio::time::timeout(Duration::from_secs(30), rx).await;
worker.pending_count.fetch_sub(1, Ordering::Relaxed);

match result {
    Ok(Ok(response)) => { /* success */ }
    Ok(Err(_)) | Err(_) => {
        worker.pending.remove(&req_id);  // Cleanup
        // Return error
    }
}
```

## Non-Goals

- Python concurrent processing (Chapter 4)
- Multiple Python worker threads (GIL prevents this anyway)

## Dependencies

- Chapter 1 (`async-eventfd`) - for async notification
- Chapter 2 (`correlation-protocol`) - for request/response matching

## Risks

1. **SPSC violation** - Need mutex or MPSC for concurrent sends
2. **Ring buffer starvation** - Many in-flight requests could fill buffer
3. **Memory growth** - Many pending requests = many oneshot channels

## Acceptance Criteria

- [ ] Dispatcher uses `Arc<WorkerConnection>` without mutex wrapper
- [ ] Multiple requests can be in-flight per worker concurrently
- [ ] Least-pending selection distributes load
- [ ] Pending count accurately tracks in-flight requests
- [ ] Backpressure handled gracefully (no panics, no hangs)
- [ ] Benchmark shows improved throughput vs Chapter 1+2 alone
