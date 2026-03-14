# Spec: correlation-protocol

**Flow ID:** `correlation-protocol`
**Epic:** `vertebra-k4k.2`
**Status:** Planning
**Parent PRD:** async-ipc-pipelining

## Summary

Enable request/response correlation using the existing `RequestHeader.id` and `ResponseHeader.id` fields. This is the foundation for pipelining - allowing multiple in-flight requests per worker.

## Code Analysis Summary

### Current State

**Protocol Already Supports Correlation!**

`src/rs/vertebra-core/src/protocol/mod.rs`:
```rust
pub struct RequestHeader {
    pub id: u64,           // <-- Already exists, unused!
    pub flags: u32,
    pub payload_len: u32,
}

pub struct ResponseHeader {
    pub id: u64,           // <-- Already exists, unused!
    pub flags: u32,
    pub payload_len: u32,
}
```

**But bridge.rs ignores it:**

`src/rs/vertebra-server/src/bridge.rs:67-71`:
```rust
let header = RequestHeader::new(
    0,  // <-- HARDCODED TO ZERO!
    flags::NONE,
    payload.len() as u32
);
```

**Python worker also ignores it:**

`src/py/vertebra/core/worker.py` - The `ResponseWriter` doesn't read the request ID from the header or include it in the response.

### What Needs to Change

1. **Rust side:** Generate unique sequence ID per request, include in header
2. **Rust side:** Create correlation map to track pending requests
3. **Python side:** Echo the request ID in response header
4. **Rust side:** Use response ID to route to correct waiter

## Requirements

### R1: Sequence ID Generation
Each worker needs a monotonic sequence counter for request IDs:
```rust
pub struct WorkerConnection {
    next_seq: AtomicU64,
    // ...
}

fn next_request_id(&self) -> u64 {
    self.next_seq.fetch_add(1, Ordering::Relaxed)
}
```

### R2: Pending Request Correlation Map
Use `DashMap` for lock-free concurrent access:
```rust
type ResponseSender = tokio::sync::oneshot::Sender<Vec<u8>>;

pub struct WorkerConnection {
    pending: DashMap<u64, ResponseSender>,
    // ...
}
```

### R3: Request Dispatch with Correlation
`bridge.rs` dispatch creates oneshot channel and registers in map:
```rust
let req_id = worker.next_request_id();
let (tx, rx) = oneshot::channel();
worker.pending.insert(req_id, tx);

let header = RequestHeader::new(req_id, flags::NONE, payload.len() as u32);
// ... send request ...

// Wait for response via oneshot
let response_bytes = rx.await?;
```

### R4: Response Router Task
Spawn background task per worker that reads responses and routes them:
```rust
async fn response_router(worker: Arc<WorkerConnection>) {
    loop {
        worker.resp_notifier.wait_readable().await?;
        worker.resp_notifier.clear()?;

        while let Some(response) = worker.resp_rx.try_recv() {
            let header = ResponseHeader::from_bytes(&response)?;
            if let Some((_, tx)) = worker.pending.remove(&header.id) {
                let _ = tx.send(response);
            }
        }
    }
}
```

### R5: Python Request ID Echo
Python worker must read request ID from header and include in response:
```python
def _handle_request(self, msg_bytes: bytes) -> bytes:
    # Parse header (first 16 bytes)
    req_id = struct.unpack('<Q', msg_bytes[:8])[0]
    # ... process ASGI ...
    # Include same ID in response header
    resp_header = struct.pack('<QII', req_id, flags, len(payload))
    return resp_header + payload
```

## Non-Goals

- Removing the mutex (Chapter 3)
- Multiple concurrent requests yet (that requires Chapter 3)
- Python concurrent processing (Chapter 4)

## Dependencies

- Chapter 1 (`async-eventfd`) - for `resp_notifier.wait_readable().await`

## Risks

1. **DashMap overhead** - Minimal, well-optimized for concurrent access
2. **Oneshot channel memory** - One channel per in-flight request, bounded by worker queue depth
3. **Response router panic** - Need graceful handling if router task dies

## Acceptance Criteria

- [ ] Requests include unique u64 sequence ID in header
- [ ] `DashMap<u64, oneshot::Sender>` tracks pending requests
- [ ] Response router task spawned per worker
- [ ] Python worker echoes request ID in response
- [ ] Responses routed to correct waiters
- [ ] Tests verify correlation under concurrent load
