# Plan: lock-free-dispatch

**Flow ID:** `lock-free-dispatch`
**Epic:** `vertebra-k4k.3`

## Phase 1: WorkerConnection Enhancements

### Task 1.1: Add pending counter
**File:** `src/rs/vertebra-server/src/worker.rs`

```rust
pub struct WorkerConnection {
    // ... existing fields ...
    pub pending_count: AtomicU32,
    req_tx: tokio::sync::Mutex<SpscSender>,  // Mutex for send only
}

impl WorkerConnection {
    pub fn increment_pending(&self) -> u32 {
        self.pending_count.fetch_add(1, Ordering::Relaxed)
    }

    pub fn decrement_pending(&self) -> u32 {
        self.pending_count.fetch_sub(1, Ordering::Relaxed)
    }

    pub fn pending(&self) -> u32 {
        self.pending_count.load(Ordering::Relaxed)
    }
}
```

### Task 1.2: Wrap SpscSender in tokio Mutex
**File:** `src/rs/vertebra-server/src/worker.rs`

Use `tokio::sync::Mutex` for async-friendly locking:
```rust
impl WorkerConnection {
    pub async fn send_request(&self, data: &[u8]) -> Result<(), ChannelError> {
        let tx = self.req_tx.lock().await;
        tx.send(data, 0)
    }
}
```

## Phase 2: Dispatcher Refactor

### Task 2.1: Remove outer Mutex, implement least-pending
**File:** `src/rs/vertebra-server/src/dispatcher.rs`

```rust
pub struct Dispatcher {
    workers: Vec<Arc<WorkerConnection>>,
}

impl Dispatcher {
    pub fn new(workers: Vec<WorkerConnection>) -> Self {
        Self {
            workers: workers.into_iter().map(Arc::new).collect(),
        }
    }

    /// Select worker with fewest in-flight requests
    pub fn select_worker(&self) -> Arc<WorkerConnection> {
        self.workers
            .iter()
            .min_by_key(|w| w.pending())
            .map(Arc::clone)
            .expect("No workers available")
    }

    pub fn workers(&self) -> &[Arc<WorkerConnection>] {
        &self.workers
    }
}
```

### Task 2.2: Start response routers for all workers
**File:** `src/rs/vertebra-server/src/lib.rs` or startup code

```rust
pub fn start_workers(dispatcher: &Dispatcher) {
    for worker in dispatcher.workers() {
        spawn_response_router(Arc::clone(worker));
    }
}
```

## Phase 3: Bridge Dispatch Without Lock

### Task 3.1: Refactor dispatch signature
**File:** `src/rs/vertebra-server/src/bridge.rs`

```rust
pub async fn dispatch(
    req: Request<hyper::body::Incoming>,
    worker: Arc<WorkerConnection>  // No mutex wrapper
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    // ... build request payload ...

    // Increment pending BEFORE send
    worker.increment_pending();

    // Generate ID and register waiter
    let req_id = worker.next_request_id();
    let (tx, rx) = oneshot::channel();
    worker.pending.insert(req_id, tx);

    // Build message with header
    let header = RequestHeader::new(req_id, flags::NONE, payload.len() as u32);
    let mut message = Vec::with_capacity(RequestHeader::SIZE + payload.len());
    message.extend_from_slice(&header.to_bytes());
    message.extend_from_slice(&payload);

    // Send (small mutex critical section)
    if let Err(e) = worker.send_request(&message).await {
        worker.decrement_pending();
        worker.pending.remove(&req_id);
        return Err(e.into());
    }
    worker.req_notifier.notify()?;

    // Await response with timeout
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        rx
    ).await;

    // ALWAYS decrement pending
    worker.decrement_pending();

    match result {
        Ok(Ok(response_bytes)) => {
            // Parse and return response
            // ...
        }
        Ok(Err(_)) => Err("Response channel closed".into()),
        Err(_) => {
            worker.pending.remove(&req_id);
            Err("Request timed out".into())
        }
    }
}
```

### Task 3.2: Update router to use dispatcher
**File:** `src/rs/vertebra-server/src/router.rs`

```rust
pub async fn handle_request(
    req: Request<hyper::body::Incoming>,
    dispatcher: Arc<Dispatcher>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let worker = dispatcher.select_worker();

    match dispatch(req, worker).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            Ok(Response::builder()
                .status(500)
                .body(Full::new(Bytes::from(e.to_string())))
                .unwrap())
        }
    }
}
```

## Phase 4: Backpressure & Resilience

### Task 4.1: Handle full ring buffer
**File:** `src/rs/vertebra-server/src/bridge.rs`

```rust
// Replace simple send with retry logic
const MAX_SEND_RETRIES: u32 = 3;
const SEND_RETRY_DELAY: Duration = Duration::from_millis(10);

async fn send_with_backpressure(
    worker: &WorkerConnection,
    message: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for attempt in 0..MAX_SEND_RETRIES {
        match worker.try_send_request(message).await {
            Ok(()) => return Ok(()),
            Err(ChannelError::Full) => {
                if attempt < MAX_SEND_RETRIES - 1 {
                    tokio::time::sleep(SEND_RETRY_DELAY).await;
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err("Ring buffer full after retries".into())
}
```

### Task 4.2: Add health monitoring
**File:** `src/rs/vertebra-server/src/worker.rs`

```rust
impl WorkerConnection {
    pub fn is_healthy(&self) -> bool {
        // Check process is alive
        // Check pending count is reasonable
        self.pending() < 1000  // Example threshold
    }
}
```

## Phase 5: Testing

### Task 5.1: Concurrent dispatch test
**File:** `src/rs/vertebra-server/tests/concurrent_dispatch.rs`

```rust
#[tokio::test]
async fn test_concurrent_requests() {
    let dispatcher = setup_test_dispatcher().await;

    // Spawn many concurrent requests
    let handles: Vec<_> = (0..100)
        .map(|i| {
            let d = Arc::clone(&dispatcher);
            tokio::spawn(async move {
                let req = build_test_request(format!("/test/{}", i));
                let worker = d.select_worker();
                dispatch(req, worker).await
            })
        })
        .collect();

    // All should complete
    for handle in handles {
        assert!(handle.await.unwrap().is_ok());
    }
}
```

## Task Dependencies

```
1.1 ─> 1.2 ─> 2.1 ─> 2.2 ─> 3.1 ─> 3.2 ─> 4.1 ─> 4.2 ─> 5.1
```

Also depends on Chapter 2 completion (correlation-protocol).

## Estimated Complexity

- **Phase 1:** Low - Add counter, wrap sender
- **Phase 2:** Medium - Dispatcher refactor
- **Phase 3:** Medium - Bridge signature change, cleanup logic
- **Phase 4:** Medium - Backpressure, health checks
- **Phase 5:** Low - Testing

## Notes

- The small mutex around `SpscSender.send()` is acceptable - it's held only for the duration of a memory copy (~microseconds)
- True lock-free would require MPSC ring, which is a larger change
- Least-pending selection prevents hot-spotting under load
- Pending counter is approximate (Relaxed ordering) but sufficient for load balancing
