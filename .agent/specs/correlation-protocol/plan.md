# Plan: correlation-protocol

**Flow ID:** `correlation-protocol`
**Epic:** `vertebra-k4k.2`

## Phase 1: WorkerConnection Structure

### Task 1.1: Create WorkerConnection type
**File:** `src/rs/vertebra-server/src/worker.rs`

Replace `Worker` struct with `WorkerConnection` that supports correlation:
```rust
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

pub type ResponseSender = oneshot::Sender<Vec<u8>>;

pub struct WorkerConnection {
    pub req_tx: SpscSender,
    pub resp_rx: SpscReceiver,
    pub req_notifier: AsyncNotifier,
    pub resp_notifier: AsyncNotifier,
    pub pending: DashMap<u64, ResponseSender>,
    next_seq: AtomicU64,
    pub pid: u32,
    pub process: std::process::Child,
}

impl WorkerConnection {
    pub fn next_request_id(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }
}
```

### Task 1.2: Add dashmap dependency
**File:** `src/rs/vertebra-server/Cargo.toml`

```toml
[dependencies]
dashmap = "5"
```

## Phase 2: Response Router

### Task 2.1: Implement response router task
**File:** `src/rs/vertebra-server/src/router.rs` (extend existing or new module)

```rust
use crate::worker::WorkerConnection;
use std::sync::Arc;
use vertebra_core::protocol::ResponseHeader;

pub async fn spawn_response_router(worker: Arc<WorkerConnection>) {
    tokio::spawn(async move {
        let mut buffer = vec![0u8; 65536];
        loop {
            // Wait for response notification
            if let Err(e) = worker.resp_notifier.wait_readable().await {
                tracing::error!("Response notifier error: {}", e);
                break;
            }
            if let Err(e) = worker.resp_notifier.clear() {
                tracing::error!("Failed to clear notification: {}", e);
            }

            // Drain all available responses
            while let Ok(len) = worker.resp_rx.try_recv(&mut buffer) {
                if len < ResponseHeader::SIZE {
                    tracing::warn!("Response too short: {} bytes", len);
                    continue;
                }

                let header = match ResponseHeader::from_bytes(&buffer[..len]) {
                    Some(h) => h,
                    None => {
                        tracing::warn!("Invalid response header");
                        continue;
                    }
                };

                // Route to waiting request
                if let Some((_, tx)) = worker.pending.remove(&header.id) {
                    let response = buffer[..len].to_vec();
                    let _ = tx.send(response);  // Ignore if receiver dropped
                } else {
                    tracing::warn!("No pending request for ID {}", header.id);
                }
            }
        }
    });
}
```

### Task 2.2: Start router when worker spawns
**File:** `src/rs/vertebra-server/src/worker.rs`

Call `spawn_response_router()` after creating `WorkerConnection`.

## Phase 3: Bridge Dispatch Update

### Task 3.1: Update dispatch to use correlation
**File:** `src/rs/vertebra-server/src/bridge.rs`

```rust
pub async fn dispatch(
    req: Request<hyper::body::Incoming>,
    worker: Arc<WorkerConnection>  // Note: no longer Mutex<Worker>
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    // ... build AsgiRequest ...

    // Generate unique request ID
    let req_id = worker.next_request_id();

    // Create oneshot for response
    let (tx, rx) = oneshot::channel();
    worker.pending.insert(req_id, tx);

    let header = RequestHeader::new(
        req_id,  // <-- Use sequence ID
        flags::NONE,
        payload.len() as u32
    );

    // ... build and send message ...
    worker.req_tx.send(&message, 0)?;
    worker.req_notifier.notify()?;

    // Wait for response via oneshot (non-blocking to runtime!)
    let response_bytes = rx.await
        .map_err(|_| "Response channel closed")?;

    // Parse response...
    let header = ResponseHeader::from_bytes(&response_bytes)?;
    let payload = &response_bytes[ResponseHeader::SIZE..];
    let asgi_resp: AsgiResponse = rmp_serde::from_slice(payload)?;

    // Build HTTP response...
}
```

### Task 3.2: Add timeout handling
**File:** `src/rs/vertebra-server/src/bridge.rs`

Use `tokio::time::timeout` to prevent indefinite waits:
```rust
let response_bytes = tokio::time::timeout(
    Duration::from_secs(30),
    rx
).await
    .map_err(|_| "Request timed out after 30s")?
    .map_err(|_| "Response channel closed")?;

// Clean up pending entry on timeout
// Note: oneshot sender dropped, entry auto-removed
```

## Phase 4: Python Worker Updates

### Task 4.1: Read request ID from header
**File:** `src/py/vertebra/core/worker.py`

Update `_handle_client_payload` to extract request ID:
```python
import struct

def _handle_client_payload(self, msg_bytes: bytes) -> None:
    # Parse header (first 16 bytes)
    if len(msg_bytes) < 16:
        return
    req_id, flags, payload_len = struct.unpack('<QII', msg_bytes[:16])
    payload = msg_bytes[16:16 + payload_len]

    # Store req_id for response
    self._current_req_id = req_id

    # ... process ASGI ...
```

### Task 4.2: Include request ID in response
**File:** `src/py/vertebra/core/worker.py`

Update `ResponseWriter._send_managed` to include ID:
```python
def _send_managed(self) -> None:
    # ... build payload ...

    # Build response header with request ID
    resp_header = struct.pack('<QII',
        self._req_id,    # Echo request ID
        0,               # flags
        len(payload)
    )

    message = resp_header + payload
    self._resp_tx.send(message)
    # Notify Rust
    os.write(self._resp_notify_fd, b'\x01\x00\x00\x00\x00\x00\x00\x00')
```

## Phase 5: Dispatcher Updates

### Task 5.1: Update RoundRobinDispatcher
**File:** `src/rs/vertebra-server/src/dispatcher.rs`

Change from `Arc<Mutex<Worker>>` to `Arc<WorkerConnection>`:
```rust
pub struct RoundRobinDispatcher {
    workers: Vec<Arc<WorkerConnection>>,  // No mutex!
    next: AtomicUsize,
}
```

Note: This task bridges into Chapter 3, but the basic change can be done here since WorkerConnection is designed for concurrent access.

## Task Dependencies

```
1.1 ─> 1.2 ─> 2.1 ─> 2.2 ─> 3.1 ─> 3.2 ─> 4.1 ─> 4.2 ─> 5.1
```

Also depends on Chapter 1 completion (async-eventfd) for `wait_readable()`.

## Estimated Complexity

- **Phase 1:** Low - Struct changes, new dependency
- **Phase 2:** Medium - Router task with proper error handling
- **Phase 3:** Medium - Dispatch refactor with oneshot channels
- **Phase 4:** Low - Python struct packing/unpacking
- **Phase 5:** Low - Dispatcher type change

## Notes

- After this chapter, requests still go through the dispatcher one at a time
- The correlation infrastructure enables Chapter 3 (lock-free dispatch)
- The oneshot pattern cleanly handles request/response matching
- DashMap provides O(1) lock-free lookup and removal
