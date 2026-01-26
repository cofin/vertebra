# Implementation Plan - AsyncNotifierBridge for Bun IPC

## Problem Statement
Bun (single-threaded JS) cannot efficiently wait on Linux eventfd. Polling approaches cap throughput at ~60 RPS. Target is >200K RPS.

## Solution
Rust background thread bridges eventfd to pipe that Bun can watch natively via net.Socket.

## Phases

### Phase 1: Rust Core Implementation
- [x] Task: Create AsyncNotifierBridge struct with shutdown_fd for instant termination
- [x] Task: Implement watcher_loop polling on 2 fds (source + shutdown) with infinite timeout
- [x] Task: Add FD_CLOEXEC on all file descriptors
- [x] Task: Add comprehensive tests (instant shutdown, signal coalescing)

### Phase 2: C ABI Layer
- [x] Task: Add VtbAsyncBridge wrapper struct to vertebra-broker
- [x] Task: Implement vtb_async_bridge_new/start/stop/close/pipe_fd/is_running exports

### Phase 3: TypeScript Integration
- [x] Task: Add FFI type signatures for vtb_async_bridge_* functions
- [x] Task: Add dlopen bindings in loadShmLib()
- [x] Task: Create ShmAsyncBridge class following ShmNotifier pattern

### Phase 4: IpcConnection Migration
- [x] Task: Replace polling runResponseLoop() with event-driven drainResponses()
- [x] Task: Use net.Socket watching pipe fd for Bun event loop integration
- [x] Task: Remove ensureResponseLoop(), responseLoopRunning flag
- [x] Task: Update close() to clean up asyncBridge and pipeSocket

## Verification
- [x] Rust tests: 68 passed (5 new async_notifier tests)
- [x] TypeScript tests: 105 passed
