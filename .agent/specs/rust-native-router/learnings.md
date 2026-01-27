## [2026-01-25 21:58] - Task 7.4: Performance Validation
- **Status:** FAILED (Target: >200K RPS, Actual: ~60 RPS)
- **Root Cause:** The `AsgiWorker` main loop uses a naive polling strategy (`await asyncio.sleep(0.001)` when idle). This introduces a minimum 1ms latency floor per poll, capping throughput and causing massive latency spikes under load.
- **Critical Insight:** High-performance IPC **cannot** rely on `asyncio.sleep()`. We must use `eventfd` (Linux) or blocking/signaling mechanisms to wake the loop immediately upon data arrival.
- **Action Item:** Create a new flow: "Async Event Loop Optimization" to implement `eventfd` notification in `AsgiWorker` and `BrokerServer`.

## [2026-01-25 23:30] - Phase 8: Consensus Analysis with Gemini 3
- **Models Consulted:** gemini-3-pro-preview (for), gemini-3-flash-preview (against)
- **Consensus Confidence:** 9/10 from both models
- **Agreed Solution:** eventfd-based notification for Linux + pipe() fallback for macOS/Windows

### Key Technical Insights

1. **asyncio Integration:**
   - Python's asyncio on Linux uses epoll, which natively supports fd watching via `loop.add_reader()`
   - This allows zero-CPU sleep with immediate wake on notification (3-10μs latency vs 1ms polling)
   - Eliminates the manual `while` loop entirely in favor of reactor callbacks

2. **Why NOT Futex/CondVar:**
   - `futex` and `pthread_cond_wait` block the OS thread
   - asyncio is single-threaded; blocking the main thread stops the event loop entirely
   - Would require separate worker thread + `call_soon_threadsafe()`, adding context-switch overhead

3. **Why NOT Adaptive Polling:**
   - Does not solve the fundamental latency floor once spin phase expires
   - Wastes CPU cycles that could be used for request processing
   - Current polling implementation is non-functional (~66 RPS), not a viable fallback

4. **FD Inheritance Pattern:**
   - Create eventfd in ProcessManager before spawning worker
   - Pass fd number via CLI argument or env var
   - fd automatically inherited by subprocess (standard Unix behavior)

5. **Resource Cleanup Critical:**
   - Named shared memory persists after process death (`/dev/shm/...`)
   - eventfd auto-closes on crash (OS handles)
   - ProcessManager MUST own shm_unlink responsibility via SIGCHLD handler
   - Use `O_TMPFILE` or immediate unlink after mmap for crash safety

### Architecture Change Summary

**Before (Broken):**
```
while running:
    payload = ring.try_recv()
    if payload:
        handle(payload)
    else:
        await asyncio.sleep(0.001)  # 1ms floor = ~66 RPS max
```

**After (Fixed):**
```
loop.add_reader(notify_fd, _on_ring_readable)  # 0 CPU idle, ~10μs wake
# Loop sleeps in epoll_wait(), wakes instantly on eventfd signal

def _on_ring_readable():
    while (payload := ring.try_recv()):
        handle(payload)
```

### Process Leakage Issues Identified

1. **IpcConnection.open() (client.ts:250-257):** Creates ShmRing.create() but no shm_unlink on failure
2. **ProcessManager.handleExit() (process-manager.ts:165):** Doesn't clean up SHM when workers crash
3. **No orphan cleanup:** Server startup should scan /dev/shm/vertebra-* and clean stale regions

## [2026-01-25] - Legacy Polling Code Removal

### Changes Made

1. **PollingTransport Removed:**
   - Deleted entire `PollingTransport` class from `async_transport.py`
   - Added `UnsupportedPlatformError` for explicit failure on Windows ProactorEventLoop
   - `create_transport()` now only returns `AsyncShmTransport`

2. **Channel.py Polling Fallbacks Removed:**
   - Deleted `_send_with_polling()` and `_recv_with_polling()` methods
   - `_do_send_recv()` now requires native timeout methods
   - `receive_sync()` now requires native timeout methods
   - Clear error messages when native methods unavailable

3. **Test Updates:**
   - Updated broker tests to use `AsyncShmTransport` instead of `PollingTransport`
   - Removed `prefer_eventfd=False` parameter from all `BrokerClientConfig` usages
   - Deleted `TestPollingTransportUvloop` class from uvloop tests

### Key Insight
The polling fallback was masking the performance problem. By removing it entirely:
- Unsupported platforms fail fast with clear errors
- No silent degradation to unusable performance
- Forces proper eventfd/pipe notification usage

## [2026-01-25] - SHM Cleanup Infrastructure (vertebra-b5l)

### Changes Made

1. **ShmRing.unlink() Added (shm.ts):**
   - Added static `unlink(name)` method to ShmRing class
   - Mirrors existing `ShmMpmcRing.unlink()` implementation
   - Wraps `vtb_shm_unlink()` C ABI function, ignores ENOENT

2. **IpcConnection.close() Cleanup (client.ts):**
   - Tracks `requestRingName` and `responseRingName` during open()
   - Calls `ShmRing.unlink()` on both during close()

3. **Python Worker SHM Cleanup (worker.py):**
   - Added `shm_unlink()` call in `stop()` for graceful shutdown
   - Registered `atexit` handler for crash safety
   - **Gotcha:** PyO3 raises `RuntimeError` not `OSError` for syscall failures
   - Fixed: `contextlib.suppress(OSError, RuntimeError)` catches both

4. **ProcessManager SHM Cleanup (process-manager.ts):**
   - Added `cleanupOrphanedShm()` static method - scans /dev/shm on startup
   - Calls cleanup before spawning any workers
   - Added cleanup to `handleExit()` for individual crashed workers
   - Control ring name pattern: `/${name}.asgi.${workerId}.control`

### PyO3 Error Handling Gotcha
When a Rust function exposed via PyO3 encounters a system error:
- It raises Python `RuntimeError`, NOT `OSError`
- Error message format: "system call failed: {description} (os error {code})"
- Must catch `RuntimeError` in addition to `OSError` for robust cleanup

## [2026-01-25] - ProcessManager Notifier Lifecycle (vertebra-adh)

### Problem
When TypeScript sends data to Python via shared-memory rings:
1. Python creates `AsyncRing.create_named()` which creates an INTERNAL notifier
2. TypeScript opens `ShmRing.open()` - same ring but NO notifier
3. Python waits on `add_reader(internal_fd)` but TypeScript never signals it
4. Result: Python never wakes up to process requests

### Solution
1. ProcessManager creates `ShmNotifier` BEFORE spawning each worker
2. Passes notifier fds via CLI: `--notify-read-fd` and `--notify-write-fd`
3. Python worker uses external fd for `add_reader()` instead of ring's internal
4. TypeScript calls `notifyWorker(workerId)` after every send
5. Module-level registry (`notifier-registry.ts`) avoids circular deps

### Files Changed

1. **notifier-registry.ts (NEW):**
   - Module-level `Map<number, ShmNotifier>` registry
   - `registerWorkerNotifier()` / `unregisterWorkerNotifier()`
   - `notifyWorker(workerId)` - called by IpcConnection after send

2. **process-manager.ts:**
   - Import from notifier-registry
   - `spawnWorker()`: Create notifier, register, pass fds via CLI
   - `stopWorker()` / `handleExit()`: Unregister and close notifier

3. **client.ts (IpcConnection):**
   - Import `notifyWorker` from notifier-registry
   - Call `notifyWorker(this.workerId)` after `control.send()` and `requestRing.send()`

4. **worker.py:**
   - Add `notify_read_fd` / `notify_write_fd` to `WorkerConfig`
   - Add `--notify-read-fd` / `--notify-write-fd` CLI args
   - `_serve_loop()`: Use external fd for `add_reader()` when provided
   - Clear external eventfd via `os.read(fd, 8)` instead of ring's clear_notification()

### Key Insight: FD Inheritance
Unix file descriptors are automatically inherited by child processes. No special IPC needed:
1. Parent creates eventfd/pipe
2. Parent passes fd NUMBER via CLI arg (just an integer)
3. Child opens same fd by number - it's already open in the process

## [2026-01-25] - Supervisor Mode Notification Fix

### Problem
When running via `vertebra run` (Rust NativeSupervisor mode), the server returned 502 Bad Gateway:
1. Rust supervisor creates ONE shared eventfd, passes to Bun via `VERTEBRA_NOTIFY_FD` env var
2. Rust sets `VERTEBRA_SKIP_PROCESS_MANAGER=1` so Bun doesn't create its own notifiers
3. Python worker expected CLI args (`--notify-read-fd`) but only received env var
4. Bun's notifier registry was empty (no ProcessManager ran)
5. Python request rings used internal notifiers that TypeScript couldn't signal

### Solution
1. **Python fallback to env var:**
   - `_resolve_notify_fd()` helper: CLI arg > env var fallback
   - Supports both ProcessManager (CLI) and supervisor (env var) modes

2. **Bun inherits supervisor notifier:**
   - On startup, if `SKIP_PROCESS_MANAGER=1`, read `VERTEBRA_NOTIFY_FD`
   - Create `ShmNotifier.fromFds()` and call `setSupervisorNotifier()`
   - `notifyWorker()` falls back to supervisor notifier

3. **Python drains ALL rings on notification:**
   - In supervisor mode, ONE fd is used for ALL rings
   - `on_control_readable()` now drains control ring AND all client request rings
   - No per-ring notifier registration needed

### Key Insight: Two Operation Modes
1. **ProcessManager Mode** (Bun as entry point):
   - Each worker has own notifier created by ProcessManager
   - Notifier fds passed via CLI args
   - Per-ring notification registration

2. **Supervisor Mode** (Python CLI entry point):
   - Single shared notifier inherited from Rust
   - Notifier fd via env var `VERTEBRA_NOTIFY_FD`
   - All rings share one notification fd

## [2026-01-25] - Notification Batching (vertebra-a6w)

### Optimizations Implemented

1. **Coalesced Notification (`hasPending` check):**
   - Before calling `notify()`, check if notifier already has a pending signal
   - If pending, skip the redundant syscall - worker will drain all messages anyway
   - Reduces syscall overhead under high load

2. **Batch Context API:**
   - `beginBatch(workerId)` - start a batch, returns commit/abort methods
   - Individual sends within batch skip notification
   - `commit()` sends single notification for all batched messages
   - Nested batches supported - only outermost commit triggers notify

3. **Streaming Send Batching:**
   - `sendOutbound()` wraps multi-frame sends in batch context
   - Single notification at end of stream instead of per-frame

### API Added to notifier-registry.ts

```typescript
// Coalesced single notify
notifyWorker(workerId)  // Skips if already pending

// Batch API
const batch = beginBatch(workerId);
try {
  for (const msg of messages) ring.send(msg);
  batch.commit();  // Single notification
} catch { batch.abort(); }

// Batch-aware notify (used by IpcConnection)
notifyWorkerIfNotBatched(workerId)  // No-op during active batch
```

### Performance Impact
- Reduces eventfd writes from N (per-message) to 1 (per-batch)
- Each eventfd write is a syscall (~1µs overhead)
- For 100-message batch: ~99µs saved per batch
- Critical for achieving >200K RPS target
