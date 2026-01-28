# Spec: async-eventfd

**Flow ID:** `async-eventfd`
**Epic:** `vertebra-k4k.1`
**Status:** Planning
**Parent PRD:** async-ipc-pipelining

## Summary

Replace all blocking eventfd operations with tokio `AsyncFd` integration. This enables true async wake-up in the HTTP server without blocking tokio worker threads.

## Code Analysis Summary

### Current State

**`src/rs/vertebra-core/src/notify.rs`**
- `Notifier` struct wraps eventfd (Linux) or pipe (other platforms)
- `notify()` - blocking `libc::write()` on line 195
- `clear()` - blocking `libc::read()` on line 241
- `wait()` - blocking `libc::poll()` on line 304
- `spin_wait_or_park_with()` - spin loop + blocking poll, lines 374-411
- No tokio/async integration - all operations are synchronous

**`src/rs/vertebra-server/src/bridge.rs`**
- Uses `worker.notifier.spin_wait_or_park_with()` on line 96
- This BLOCKS the tokio task while waiting for Python response
- The entire async runtime thread is stalled

**`src/rs/vertebra-core/src/async_ring.rs`**
- `AsyncRing` wraps `SpscRing` + `Notifier`
- `recv_hybrid()` uses `spin_wait_or_park_with()` - still blocking
- Despite name "Async", it's not actually tokio-async

### Problem

The `spin_wait_or_park_with` pattern blocks the tokio worker thread:
```rust
// bridge.rs:96 - THIS BLOCKS THE ASYNC RUNTIME
let has_data = worker.notifier.spin_wait_or_park_with(5, 30_000, || {
    worker.resp_rx.has_data()
})?;
```

When a request is waiting for Python response, the entire tokio task is blocked. This means other HTTP requests cannot be processed on that thread. With default tokio multi-thread runtime (1 thread per CPU core), this severely limits concurrency.

### Target State

True async notification using `tokio::io::unix::AsyncFd`:

```rust
// New async wait method
pub async fn wait_readable(&self) -> io::Result<()> {
    let async_fd = AsyncFd::new(self.fd.as_raw_fd())?;
    async_fd.readable().await?.clear_ready();
    Ok(())
}
```

Bridge dispatch becomes fully async:
```rust
// bridge.rs - NO BLOCKING
loop {
    if worker.resp_rx.has_data() {
        break;
    }
    worker.notifier.wait_readable().await?;
}
```

## Requirements

### R1: AsyncNotifier Type
Create new `AsyncNotifier` struct in `vertebra-core` that wraps eventfd with `tokio::io::unix::AsyncFd`.

- `async fn wait_readable(&self) -> io::Result<()>` - async wait for readable
- `fn notify(&self) -> io::Result<()>` - non-blocking notify (same as current)
- `fn clear(&self) -> io::Result<()>` - non-blocking clear
- `fn has_pending(&self) -> bool` - non-blocking check

### R2: Separate Eventfds for Request/Response
Currently one eventfd handles both directions. Create two eventfds:
- `req_notify` - Rust→Python "request available" signal
- `resp_notify` - Python→Rust "response available" signal

### R3: Update Worker Struct
Modify `Worker` in `vertebra-server/src/worker.rs`:
```rust
pub struct Worker {
    pub req_tx: SpscSender,
    pub resp_rx: SpscReceiver,
    pub req_notifier: AsyncNotifier,   // Rust→Python
    pub resp_notifier: AsyncNotifier,  // Python→Rust
    pub pid: u32,
    pub process: std::process::Child,
}
```

### R4: Update Bridge Dispatch
Replace blocking wait with async:
```rust
pub async fn dispatch(...) -> Result<Response<...>, ...> {
    // ... send request ...
    worker.req_notifier.notify()?;

    // Async wait for response
    loop {
        if worker.resp_rx.has_data() {
            break;
        }
        worker.resp_notifier.wait_readable().await?;
        worker.resp_notifier.clear()?;
    }
    // ... process response ...
}
```

### R5: Update Python Worker
Modify Python worker to use separate notification FDs:
- Read from `REQ_NOTIFY_FD` for incoming requests
- Write to `RESP_NOTIFY_FD` after sending response

### R6: Remove Spin-Wait Code
After async integration works:
- Remove `spin_wait_or_park()` method
- Remove `spin_wait_or_park_with()` method
- Keep `wait()` for non-tokio contexts if needed

## Non-Goals

- Request/response correlation (Chapter 2)
- Removing mutex (Chapter 3)
- Python concurrent processing (Chapter 4)

## Dependencies

None - this is the foundation chapter.

## Risks

1. **Cross-process AsyncFd** - AsyncFd wraps RawFd, need to verify it works with inherited FDs across fork
2. **Platform compatibility** - AsyncFd is Unix-only, need cfg guards for Windows
3. **Tokio version** - AsyncFd API may vary between tokio versions

## Acceptance Criteria

- [ ] `AsyncNotifier` type with `wait_readable()` async method
- [ ] Separate eventfds for request/response directions
- [ ] Bridge dispatch uses async wait, not blocking
- [ ] Python worker uses separate notification FDs
- [ ] All spin-wait code removed from bridge path
- [ ] Tests pass for async notification pattern
- [ ] Benchmark shows no more uniform ~21 RPS ceiling
