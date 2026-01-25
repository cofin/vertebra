# Task Checklist Report: ipc-performance-optimization

- Generated: 2026-01-24
- Source: `specs/active/ipc-performance-optimization/tasks.md`
- Total tasks: 162
- Done: 162
- Pending: 0

## Checklist

# Tasks: World-Class IPC Performance Optimization

## Overview

## Status Update (2026-01-24)






## PHASE 1: Zero-Copy Foundation + Supervisor FD Inheritance

### Task 1.1: Add `InheritableNotifier` to Rust Core


#### 1.1.1 Create `InheritableNotifier` struct
- [x] Add new struct `InheritableNotifier` after existing `Notifier` (line ~242)
- [x] Include fields for Linux (eventfd) and other platforms (pipe pair)
- [x] **CRITICAL**: Do NOT use `EFD_CLOEXEC` flag



#### 1.1.2 Add constructor for supervisor use
- [x] Implement `InheritableNotifier::new()` - creates FD before fork
- [x] Linux: `eventfd(0, EFD_NONBLOCK)` only
- [x] macOS/BSD: `pipe()` without `FD_CLOEXEC`

#### 1.1.3 Add constructor for child process use
- [x] Implement `InheritableNotifier::from_raw_fd(fd: RawFd)` for children
- [x] Children receive FD number via env var, reconstruct handle

#### 1.1.4 Add `as_raw_fd()` method
- [x] Return raw FD for passing to children via env var

#### 1.1.5 Add unit tests for FD inheritance
- [x] Test: Create notifier, fork process, verify FD accessible in child
- [x] Test: notify() in parent, clear() in child works
- [x] Test: Verify `EFD_CLOEXEC` is NOT set (use `fcntl(fd, F_GETFD)`)

# Verify with strace on Linux:


### Task 1.2: Modify `NativeSupervisor` for FD Creation Before Fork


#### 1.2.1 Add FD storage fields to `NativeSupervisor`
- [x] Add `notify_fd: Option<RawFd>` field (line ~12)
- [x] Add `shm_fds: Vec<RawFd>` field for shared memory FDs

#### 1.2.2 Create FDs BEFORE spawning children
- [x] Modify `start()` method (line 26-86)
- [x] Create `InheritableNotifier` BEFORE `Command::new()` calls
- [x] Create shared memory regions BEFORE spawning




#### 1.2.3 Pass FD numbers via environment variables
- [x] Set `VERTEBRA_NOTIFY_FD` env var before Bun spawn
- [x] Set `VERTEBRA_SHM_FD` env var for shared memory region
- [x] Set `VERTEBRA_RING_FD` env var for ring buffer FD

#### 1.2.4 Same for Python worker spawning
- [x] Modify worker spawn (lines 64-76)
- [x] Pass same FD numbers to Python workers



#### 1.2.5 Add FD validation on startup
- [x] Verify FD is valid after creation
- [x] Log FD numbers being passed to children



### Task 1.3: Add Cache-Line Alignment to Ring Buffer Headers


#### 1.3.1 Update ring header structs
- [x] Add `#[repr(C, align(64))]` to `ShmHeader` struct
- [x] Verify struct is in `src/rs/vertebra-core/src/buffer.rs`

#### 1.3.2 Add padding between producer/consumer fields
- [x] Add 64-byte padding between `read_head` and `write_head`
- [x] Prevents false sharing between producer and consumer cores


#[repr(C, align(64))]

#### 1.3.3 Add alignment verification test
- [x] Test: Verify `ShmHeader` is 64-byte aligned
- [x] Test: Verify `read_head` and `write_head` are on different cache lines



### Task 1.4: Create `BufferPool` Class in TypeScript


#### 1.4.1 Add `BufferPool` class (after line 297)
- [x] Create class with pre-allocated `ArrayBuffer` set
- [x] Constructor takes `poolSize` and `bufferCapacity` parameters
- [x] Pre-allocate all buffers at construction time



#### 1.4.2 Implement `acquire()` method
- [x] Return buffer from pool
- [x] Handle pool exhaustion (wait or allocate fallback)
- [x] Track buffer ownership

#### 1.4.3 Implement `release()` method
- [x] Return buffer to pool
- [x] Validate buffer belongs to pool
- [x] Reset buffer state if needed

#### 1.4.4 Add unit tests for BufferPool
- [x] Test: Acquire returns buffer
- [x] Test: Release makes buffer available again
- [x] Test: Pool exhaustion handling
- [x] Test: Zero allocations after warmup




### Task 1.5: Integrate BufferPool with `ShmRing.recv()`


#### 1.5.1 Remove per-message allocation in recv()



#### 1.5.2 Add pool parameter to recv()
- [x] Make pool parameter optional for backwards compatibility
- [x] Use pool.acquire() if pool provided
- [x] Fall back to allocation if pool exhausted

#### 1.5.3 Document buffer lifecycle
- [x] Caller is responsible for calling `pool.release(buffer)` after use
- [x] Add JSDoc comments explaining ownership



### Task 1.6: Update Python Supervisor to Pass FD Numbers


#### 1.6.1 Read FD numbers from Rust supervisor
- [x] After `NativeSupervisor.start()`, retrieve FD numbers
- [x] May need to add PyO3 method to return FD numbers

#### 1.6.2 Store FDs for worker use
- [x] Store in supervisor state for late-spawned workers



### Task 1.7: Update Python Channel for Zero-Copy Buffer Returns


#### 1.7.1 REMOVE `_BufferProxy` class (DEPRECATED)








- [x] Delete entire `_BufferProxy` class (lines 144-167)

#### 1.7.2 Update `BufferView.as_memoryview()` to remove _BufferProxy fallback



- [x] Remove `_BufferProxy` imports/references from lines 67-73

#### 1.7.3 Ensure both sync and async paths use memoryview
- [x] Verify `BufferView.as_memoryview()` used in sync methods
- [x] Verify same in async methods



### Task 1.8: Phase 1 Integration Verification

- [x] Run `make test` - Python tests pass
- [x] Run `make js-test` - TypeScript tests pass
- [x] Run `make rs-test` - Rust tests pass
- [x] Run `make lint-all` - No lint errors

# Python: Verify zero allocations in recv() hot path
# ... test recv() loop

# TypeScript: Verify buffer pool usage


## PHASE 2: Event-Driven Wakeups (Sub-10μs Target)

### Task 2.1: REMOVE Polling from Bun BrokerServer (CRITICAL)


#### 2.1.1 REMOVE `setInterval(poll, 1)` (DEPRECATED)


- [x] Delete lines 53-55 completely

#### 2.1.2 Add inherited FD event loop integration
- [x] Read `VERTEBRA_NOTIFY_FD` from environment
- [x] Use `Bun.file(fd).readable` for event-driven wakeup



#### 2.1.3 Verify no CPU burn when idle
- [x] Test: Broker with no clients uses near-zero CPU
- [x] Test: Add performance counter for poll() calls

# Verify no setInterval in code:
# Should only show conditional fallback


### Task 2.2: Add Event-Driven Client Notification in Broker


#### 2.2.1 Notify clients via inherited eventfd when response ready
- [x] After sending response to ring buffer, call `notifier.notify()`
- [x] Client's event loop wakes up immediately

#### 2.2.2 Add notification FD to client context
- [x] Store client's notify FD in `ClientContext` type (line 19-24)





### Task 2.3: REMOVE asyncio.sleep() from Python AsyncShmTransport (CRITICAL)


#### 2.3.1 Update transport to use `asyncio.add_reader()`
- [x] Read `VERTEBRA_NOTIFY_FD` from environment
- [x] Use `loop.add_reader(notify_fd, callback)` for wakeups


#### 2.3.2 Clear eventfd after wakeup
- [x] Read 8 bytes from eventfd to reset counter
- [x] Prevent spurious wakeups

#### 2.3.3 Test with uvloop
- [x] Verify same behavior with uvloop installed
- [x] `add_reader()` should work identically



### Task 2.4: REMOVE `asyncio.to_thread()` from Sync Path (CRITICAL)


#### 2.4.1 REMOVE `asyncio.to_thread()` from `Channel.send()` (DEPRECATED)





- [x] Rewrite `send()` to avoid thread pool
- [x] Use eventfd-based `_wait_for_response()` helper

#### 2.4.2 REMOVE `asyncio.to_thread()` from `Channel.receive()` (DEPRECATED)


- [x] Rewrite to use event-driven wakeup

#### 2.4.3 REMOVE `asyncio.to_thread()` from `Channel.respond()` (DEPRECATED)


- [x] Rewrite to not use thread pool

# Verify no asyncio.to_thread in channel.py:
# Should return empty


### Task 2.5: Optimize Sync Methods in Channel


#### 2.5.1 Use `select.select()` for sync blocking
- [x] Replace `time.sleep(0.001)` polling with `select.select([fd], [], [], timeout)`
- [x] Direct syscall, no thread pool overhead


# ...

#### 2.5.2 Read notify FD from environment
- [x] `notify_fd = int(os.environ.get("VERTEBRA_NOTIFY_FD", "0"))`
- [x] Fall back to sleep polling if no FD available



### Task 2.6: REMOVE `asyncio.sleep(0.001)` from Python Worker (CRITICAL)


#### 2.6.1 REMOVE polling sleep in serve loop (DEPRECATED)




- [x] Replace sleep with `add_reader()` on notify FD
- [x] Handle shutdown signals properly

#### 2.6.2 Handle shutdown properly with eventfd
- [x] Write to eventfd on shutdown to wake worker
- [x] Check `_running` flag after wakeup

# Verify no unconditional asyncio.sleep in worker.py:
# Should only show conditional fallback


### Task 2.7: Phase 2 Latency Verification

- [x] Run latency benchmark: verify p99 < 10μs
- [x] Verify idle CPU usage < 0.1% per core

# Create latency benchmark script

# Check CPU usage when idle
# CPU should be near 0% when no requests


## PHASE 3: FlatBuffers Control Plane (Zero Parse Cost)

### Task 3.1: Create FlatBuffers Schema


- [x] Create `src/schema/` directory
- [x] Define `Request` table
- [x] Define `Response` table
- [x] Define `ControlMessage` table
- [x] Define `Stats` table
- [x] Define `RequestHeader` inline struct









### Task 3.2: Generate Rust FlatBuffers Code


#### 3.2.1 Add flatbuffers crate dependency
- [x] Add to `src/rs/vertebra-core/Cargo.toml`:

#### 3.2.2 Add code generation to build.rs
- [x] Create/modify `src/rs/vertebra-core/build.rs`
- [x] Run `flatc` during build

#### 3.2.3 Create protocol module
- [x] Create `src/rs/vertebra-core/src/protocol/mod.rs`
- [x] Include generated code



### Task 3.3: Generate Python FlatBuffers Code


#### 3.3.1 Add flatbuffers dependency
- [x] Add to `pyproject.toml`:

#### 3.3.2 Create generation script
- [x] Create `scripts/generate_flatbuffers.py`
- [x] Run `flatc --python` on schema

#### 3.3.3 Create protocol module
- [x] Create `src/py/vertebra/protocol/flatbuffers/__init__.py`



### Task 3.4: Generate TypeScript FlatBuffers Code


#### 3.4.1 Add flatbuffers dependency
- [x] Add to `src/ts/package.json`:

#### 3.4.2 Add generation to build
- [x] Add npm script for flatc generation
- [x] Run during `bun run build`

#### 3.4.3 Create protocol module
- [x] Create `src/ts/src/protocol/flatbuffers/index.ts`



### Task 3.5: Integrate FlatBuffers Headers with MessagePack Payloads


#### 3.5.1 Update Rust message encoding
- [x] Use FlatBuffers for headers
- [x] Keep MessagePack for dynamic payloads

#### 3.5.2 Update Python message encoding
- [x] Same hybrid approach

#### 3.5.3 Update TypeScript message encoding
- [x] Same hybrid approach



### Task 3.6: REMOVE Dead Code from serialization.ts (DEPRECATED)


#### 3.6.1 REMOVE `frameMessage` function


- [x] Delete `frameMessage` function (lines 14-22)

#### 3.6.2 REMOVE `deframeMessage` function


- [x] Delete `deframeMessage` function (lines 24-35)

#### 3.6.3 Update test file

- [x] Remove tests for `frameMessage` and `deframeMessage` (lines 4-6, 20-21)
- [x] Or delete entire test file if only testing removed functions

# Verify removed:
# Should return empty


### Task 3.7: Phase 3 Verification

- [x] Run `make test-all` - all tests pass
- [x] Verify p99 latency still < 10μs
- [x] Verify CPU usage reduced (less parsing work)


## PHASE 4: Request Multiplexing + Polish

### Task 4.1: Add Correlation ID Tracking in TypeScript


#### 4.1.1 Replace single pending with Map
- [x] Change from `pending: DeferredPromise` to `pending: Map<number, DeferredPromise>`
- [x] Generate unique correlation IDs

#### 4.1.2 Allow 64 concurrent in-flight requests
- [x] Configure max concurrent requests
- [x] Handle backpressure when limit reached

#### 4.1.3 Handle out-of-order responses
- [x] Match responses by correlation ID
- [x] Resolve correct promise



### Task 4.2: Add Multiplexing Support in Python


#### 4.2.1 Thread-safe pending map for sync
- [x] Use `threading.Lock` protected dict
- [x] Or use `concurrent.futures.Future`

#### 4.2.2 asyncio.Future-based tracking for async
- [x] Use `dict[int, asyncio.Future]` for pending requests
- [x] Resolve futures when responses arrive

#### 4.2.3 Both paths support concurrent requests
- [x] Test with 64 concurrent requests



### Task 4.3: CONSIDER nanoarrow Removal (Optional)


#### 4.3.1 Evaluate nanoarrow usage
- [x] Check if nanoarrow is actually used in production
- [x] Lines 78-79, 101-155 reference nanoarrow

#### 4.3.2 If removing, make pyarrow required
- [x] Update `pyproject.toml` to make pyarrow non-optional
- [x] Remove nanoarrow fallback code



### Task 4.4: Final Benchmark Suite

#### 4.4.1 Latency benchmark
- [x] p50 < 5μs
- [x] p99 < 10μs
- [x] p999 < 50μs

#### 4.4.2 Throughput benchmark
- [x] > 1M msg/sec per connection

#### 4.4.3 Allocation benchmark
- [x] Zero allocations in hot path

#### 4.4.4 Sync/Async parity benchmark
- [x] Both achieve same latency targets


### Task 4.5: Final Integration Verification

- [x] `make test` passes
- [x] `make js-test` passes
- [x] `make rs-test` passes
- [x] `make lint-all` passes
- [x] All deprecation removals verified (see checklist below)


## DEPRECATION REMOVAL CHECKLIST

### Code Removals (Must Complete)


### Verification Commands

# Verify all deprecated code is removed:

# All commands above should return empty (no matches)


## VERIFICATION MATRIX

### Phase 1 Complete When:
- [x] `make test-all` passes
- [x] Zero allocations in `recv()` hot path (profiler verified)
- [x] FD inheritance works across fork (strace verified)
- [x] `_BufferProxy` class removed
- [x] `EFD_CLOEXEC` not present in new `InheritableNotifier`

### Phase 2 Complete When:
- [x] No `setInterval` in server.ts (except conditional fallback)
- [x] No `asyncio.sleep` in worker.py (except conditional fallback)
- [x] No `asyncio.to_thread` in channel.py
- [x] No `time.sleep(0.001)` polling in channel.py
- [x] p99 latency < 10μs
- [x] Idle CPU < 0.1%

### Phase 3 Complete When:
- [x] FlatBuffers schema defined in `src/schema/ipc.fbs`
- [x] Code generated for Rust, Python, TypeScript
- [x] Control plane uses FlatBuffers
- [x] `frameMessage`/`deframeMessage` removed

### Phase 4 Complete When:
- [x] 64+ concurrent requests supported
- [x] Throughput > 1M msg/sec
- [x] Sync and async achieve same latency
- [x] All deprecation checklist items complete


## FINAL SIGN-OFF
