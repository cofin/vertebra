# Plan: async-eventfd

**Flow ID:** `async-eventfd`
**Epic:** `vertebra-k4k.1`

## Phase 1: AsyncNotifier Implementation

### Task 1.1: Create AsyncNotifier struct
**File:** `src/rs/vertebra-core/src/async_notify.rs` (new)

Create new async-aware notification type:
```rust
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use tokio::io::unix::AsyncFd;

pub struct AsyncNotifier {
    fd: OwnedFd,
    async_fd: AsyncFd<RawFd>,
}

impl AsyncNotifier {
    pub fn new() -> io::Result<Self>;
    pub unsafe fn from_raw_fd(fd: RawFd) -> io::Result<Self>;
    pub fn as_raw_fd(&self) -> RawFd;
    pub fn notify(&self) -> io::Result<()>;
    pub fn clear(&self) -> io::Result<()>;
    pub fn has_pending(&self) -> bool;
    pub async fn wait_readable(&self) -> io::Result<()>;
}
```

**Test:** Unit test for async notification roundtrip using `#[tokio::test]`

### Task 1.2: Add tokio dependency to vertebra-core
**File:** `src/rs/vertebra-core/Cargo.toml`

Add optional tokio feature:
```toml
[dependencies]
tokio = { version = "1", features = ["io-util"], optional = true }

[features]
default = []
async = ["tokio"]
```

### Task 1.3: Export AsyncNotifier
**File:** `src/rs/vertebra-core/src/lib.rs`

Add conditional export:
```rust
#[cfg(feature = "async")]
pub mod async_notify;
#[cfg(feature = "async")]
pub use async_notify::AsyncNotifier;
```

## Phase 2: Dual Eventfd Setup

### Task 2.1: Update Worker struct
**File:** `src/rs/vertebra-server/src/worker.rs`

Replace single notifier with two:
```rust
pub struct Worker {
    pub req_tx: SpscSender,
    pub resp_rx: SpscReceiver,
    pub req_notifier: AsyncNotifier,   // Rust notifies Python
    pub resp_notifier: AsyncNotifier,  // Python notifies Rust
    pub pid: u32,
    pub process: std::process::Child,
}
```

### Task 2.2: Update Worker spawn
**File:** `src/rs/vertebra-server/src/worker.rs`

Create two eventfds and pass both FDs to Python:
```rust
impl Worker {
    pub fn spawn(...) -> Result<Self, ...> {
        let req_notifier = AsyncNotifier::new()?;
        let resp_notifier = AsyncNotifier::new()?;

        // Pass FDs via environment
        cmd.env("VERTEBRA_REQ_NOTIFY_FD", req_notifier.as_raw_fd().to_string());
        cmd.env("VERTEBRA_RESP_NOTIFY_FD", resp_notifier.as_raw_fd().to_string());
        // ...
    }
}
```

### Task 2.3: Update vertebra-server Cargo.toml
**File:** `src/rs/vertebra-server/Cargo.toml`

Enable async feature in vertebra-core dependency:
```toml
[dependencies]
vertebra-core = { path = "../vertebra-core", features = ["async"] }
```

## Phase 3: Async Bridge Dispatch

### Task 3.1: Refactor dispatch to async wait
**File:** `src/rs/vertebra-server/src/bridge.rs`

Replace spin-wait with async:
```rust
pub async fn dispatch(
    req: Request<hyper::body::Incoming>,
    worker: Arc<Mutex<Worker>>
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    // ... build and send request ...

    let worker = worker.lock().await;
    worker.req_tx.send(&message, 0)?;
    worker.req_notifier.notify()?;

    // Async wait for response - NO BLOCKING
    let mut resp_buffer = vec![0u8; 65536];
    let len = loop {
        match worker.resp_rx.try_recv(&mut resp_buffer) {
            Ok(n) => break n,
            Err(ChannelError::Empty) => {
                worker.resp_notifier.wait_readable().await?;
                worker.resp_notifier.clear()?;
            }
            Err(e) => return Err(e.into()),
        }
    };

    // ... process response ...
}
```

### Task 3.2: Remove spin_wait import
**File:** `src/rs/vertebra-server/src/bridge.rs`

Remove unused imports:
```rust
// REMOVE: use vertebra_core::channel::ChannelError;  (keep this one)
// The spin_wait_or_park_with call is removed
```

## Phase 4: Python Worker Updates

### Task 4.1: Update Python worker to use dual FDs
**File:** `src/py/vertebra/core/worker.py`

Read both FDs from environment and use them appropriately:
```python
class Worker:
    def __init__(self):
        self.req_notify_fd = int(os.environ["VERTEBRA_REQ_NOTIFY_FD"])
        self.resp_notify_fd = int(os.environ["VERTEBRA_RESP_NOTIFY_FD"])

    async def run(self):
        loop = asyncio.get_event_loop()
        loop.add_reader(self.req_notify_fd, self._on_request_ready)
        # ...

    def _send_response(self, response_bytes):
        self.resp_tx.send(response_bytes)
        # Notify Rust that response is ready
        os.write(self.resp_notify_fd, b'\x01\x00\x00\x00\x00\x00\x00\x00')
```

### Task 4.2: Clear request notification
**File:** `src/py/vertebra/core/worker.py`

Clear req notification FD when reading request:
```python
def _on_request_ready(self):
    # Clear notification
    os.read(self.req_notify_fd, 8)
    # ... process requests ...
```

## Phase 5: Cleanup

### Task 5.1: Remove spin-wait methods from bridge path
**Files:** Various

Verify no code in `vertebra-server` uses:
- `spin_wait_or_park()`
- `spin_wait_or_park_with()`

Note: Keep these methods in `vertebra-core` for other consumers.

### Task 5.2: Update tests
**File:** `src/rs/vertebra-server/tests/`

Add integration test for async notification flow.

## Task Dependencies

```
1.1 ─┬─> 1.2 ─> 1.3
     │
     └─────────────> 2.1 ─> 2.2 ─> 2.3 ─> 3.1 ─> 3.2 ─> 4.1 ─> 4.2 ─> 5.1 ─> 5.2
```

## Estimated Complexity

- **Phase 1:** Medium - New async type, tokio integration
- **Phase 2:** Low - Struct changes, env var passing
- **Phase 3:** Medium - Core dispatch refactor
- **Phase 4:** Low - Python env var reading, write() calls
- **Phase 5:** Low - Cleanup and tests

## Notes

- This chapter does NOT change the mutex pattern (Chapter 3)
- Each request still holds the mutex for its full lifetime
- But the wait is now async, allowing other tokio tasks to progress
- Full pipelining requires Chapter 2 (correlation) + Chapter 3 (lock-free)
