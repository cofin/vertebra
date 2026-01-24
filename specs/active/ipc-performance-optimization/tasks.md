# Tasks: World-Class IPC Performance Optimization

## Overview

- **Complexity**: High
- **Total Tasks**: 52
- **Phases**: 4
- **Estimated Checkpoints**: 12+

---

## PHASE 1: Zero-Copy Foundation + Supervisor FD Inheritance

### Task 1.1: Add `InheritableNotifier` to Rust Core

**File**: `src/rs/vertebra-core/src/notify.rs`

#### 1.1.1 Create `InheritableNotifier` struct
- [ ] Add new struct `InheritableNotifier` after existing `Notifier` (line ~242)
- [ ] Include fields for Linux (eventfd) and other platforms (pipe pair)
- [ ] **CRITICAL**: Do NOT use `EFD_CLOEXEC` flag

**Before** (line 61):
```rust
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
```

**After** (new `InheritableNotifier::new_eventfd()`):
```rust
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };  // NO EFD_CLOEXEC
```

#### 1.1.2 Add constructor for supervisor use
- [ ] Implement `InheritableNotifier::new()` - creates FD before fork
- [ ] Linux: `eventfd(0, EFD_NONBLOCK)` only
- [ ] macOS/BSD: `pipe()` without `FD_CLOEXEC`

#### 1.1.3 Add constructor for child process use
- [ ] Implement `InheritableNotifier::from_raw_fd(fd: RawFd)` for children
- [ ] Children receive FD number via env var, reconstruct handle

#### 1.1.4 Add `as_raw_fd()` method
- [ ] Return raw FD for passing to children via env var

#### 1.1.5 Add unit tests for FD inheritance
- [ ] Test: Create notifier, fork process, verify FD accessible in child
- [ ] Test: notify() in parent, clear() in child works
- [ ] Test: Verify `EFD_CLOEXEC` is NOT set (use `fcntl(fd, F_GETFD)`)

**Verification**:
```bash
cd src/rs && cargo test --package vertebra-core notifier -- --nocapture
# Verify with strace on Linux:
strace -f -e eventfd2 cargo test --package vertebra-core notifier 2>&1 | grep -v CLOEXEC
```

---

### Task 1.2: Modify `NativeSupervisor` for FD Creation Before Fork

**File**: `src/rs/vertebra-py/src/supervisor.rs`

#### 1.2.1 Add FD storage fields to `NativeSupervisor`
- [ ] Add `notify_fd: Option<RawFd>` field (line ~12)
- [ ] Add `shm_fds: Vec<RawFd>` field for shared memory FDs

#### 1.2.2 Create FDs BEFORE spawning children
- [ ] Modify `start()` method (line 26-86)
- [ ] Create `InheritableNotifier` BEFORE `Command::new()` calls
- [ ] Create shared memory regions BEFORE spawning

**Before** (lines 46-55):
```rust
let bun = Command::new(&engine_path)
    .args(&engine_args)
    // .env("VERTEBRA_CONFIG", ...)
    .spawn()
```

**After**:
```rust
// Create notification FD BEFORE spawning
let notifier = InheritableNotifier::new()?;
let notify_fd = notifier.as_raw_fd();

let bun = Command::new(&engine_path)
    .args(&engine_args)
    .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string())
    .env("VERTEBRA_SHM_FD", shm_fd.to_string())
    .spawn()
```

#### 1.2.3 Pass FD numbers via environment variables
- [ ] Set `VERTEBRA_NOTIFY_FD` env var before Bun spawn
- [ ] Set `VERTEBRA_SHM_FD` env var for shared memory region
- [ ] Set `VERTEBRA_RING_FD` env var for ring buffer FD

#### 1.2.4 Same for Python worker spawning
- [ ] Modify worker spawn (lines 64-76)
- [ ] Pass same FD numbers to Python workers

**Before** (lines 65-75):
```rust
let worker = Command::new(&python_executable)
    .arg("-m")
    .arg("vertebra.server.worker")
    // .env(...)
    .spawn()
```

**After**:
```rust
let worker = Command::new(&python_executable)
    .arg("-m")
    .arg("vertebra.server.worker")
    .env("VERTEBRA_NOTIFY_FD", notify_fd.to_string())
    .env("VERTEBRA_SHM_FD", shm_fd.to_string())
    .spawn()
```

#### 1.2.5 Add FD validation on startup
- [ ] Verify FD is valid after creation
- [ ] Log FD numbers being passed to children

**Verification**:
```bash
cd src/rs && cargo test --package vertebra-py supervisor
cd src/rs && cargo build --package vertebra-py
```

---

### Task 1.3: Add Cache-Line Alignment to Ring Buffer Headers

**File**: `src/rs/vertebra-core/src/channel.rs`

#### 1.3.1 Update ring header structs
- [ ] Add `#[repr(C, align(64))]` to `ShmHeader` struct
- [ ] Verify struct is in `src/rs/vertebra-core/src/buffer.rs`

#### 1.3.2 Add padding between producer/consumer fields
- [ ] Add 64-byte padding between `read_head` and `write_head`
- [ ] Prevents false sharing between producer and consumer cores

**Before**:
```rust
pub read_head: CacheLine<AtomicU64>,
pub write_head: CacheLine<AtomicU64>,
```

**After**:
```rust
#[repr(C, align(64))]
pub struct ShmHeader {
    // ... existing fields ...
    pub read_head: CacheLine<AtomicU64>,
    _pad1: [u8; 64],  // Cache line padding
    pub write_head: CacheLine<AtomicU64>,
    _pad2: [u8; 64],  // Cache line padding
    // ...
}
```

#### 1.3.3 Add alignment verification test
- [ ] Test: Verify `ShmHeader` is 64-byte aligned
- [ ] Test: Verify `read_head` and `write_head` are on different cache lines

**Verification**:
```bash
cd src/rs && cargo test --package vertebra-core alignment
```

---

### Task 1.4: Create `BufferPool` Class in TypeScript

**File**: `src/ts/src/broker/shm.ts`

#### 1.4.1 Add `BufferPool` class (after line 297)
- [ ] Create class with pre-allocated `ArrayBuffer` set
- [ ] Constructor takes `poolSize` and `bufferCapacity` parameters
- [ ] Pre-allocate all buffers at construction time

```typescript
export class BufferPool {
  private buffers: Uint8Array[];
  private available: Uint8Array[];
  private capacity: number;

  constructor(poolSize: number, capacity: number) {
    this.capacity = capacity;
    this.buffers = [];
    this.available = [];
    for (let i = 0; i < poolSize; i++) {
      const buf = new Uint8Array(capacity);
      this.buffers.push(buf);
      this.available.push(buf);
    }
  }
  // ...
}
```

#### 1.4.2 Implement `acquire()` method
- [ ] Return buffer from pool
- [ ] Handle pool exhaustion (wait or allocate fallback)
- [ ] Track buffer ownership

#### 1.4.3 Implement `release()` method
- [ ] Return buffer to pool
- [ ] Validate buffer belongs to pool
- [ ] Reset buffer state if needed

#### 1.4.4 Add unit tests for BufferPool
- [ ] Test: Acquire returns buffer
- [ ] Test: Release makes buffer available again
- [ ] Test: Pool exhaustion handling
- [ ] Test: Zero allocations after warmup

**File**: `src/ts/tests/unit/buffer-pool.test.ts` (new file)

**Verification**:
```bash
cd src/ts && bun test src/tests/unit/buffer-pool.test.ts
```

---

### Task 1.5: Integrate BufferPool with `ShmRing.recv()`

**File**: `src/ts/src/broker/shm.ts`

#### 1.5.1 Remove per-message allocation in recv()
**Location**: Lines 272-291

**Before** (line 273):
```typescript
recv(timeoutMs = 0): { data: Uint8Array; flags: number } | null {
  const buffer = new Uint8Array(this.capacity);  // ALLOCATION EVERY CALL
```

**After**:
```typescript
recv(timeoutMs = 0, pool?: BufferPool): { data: Uint8Array; flags: number } | null {
  const buffer = pool?.acquire() ?? new Uint8Array(this.capacity);
```

#### 1.5.2 Add pool parameter to recv()
- [ ] Make pool parameter optional for backwards compatibility
- [ ] Use pool.acquire() if pool provided
- [ ] Fall back to allocation if pool exhausted

#### 1.5.3 Document buffer lifecycle
- [ ] Caller is responsible for calling `pool.release(buffer)` after use
- [ ] Add JSDoc comments explaining ownership

**Verification**:
```bash
cd src/ts && bun test src/tests/unit/shm.test.ts
```

---

### Task 1.6: Update Python Supervisor to Pass FD Numbers

**File**: `src/py/vertebra/supervisor.py`

#### 1.6.1 Read FD numbers from Rust supervisor
- [ ] After `NativeSupervisor.start()`, retrieve FD numbers
- [ ] May need to add PyO3 method to return FD numbers

#### 1.6.2 Store FDs for worker use
- [ ] Store in supervisor state for late-spawned workers

**Verification**:
```bash
uv run pytest src/py/tests/unit/test_supervisor.py -v
```

---

### Task 1.7: Update Python Channel for Zero-Copy Buffer Returns

**File**: `src/py/vertebra/ipc/channel.py`

#### 1.7.1 REMOVE `_BufferProxy` class (DEPRECATED)
**Location**: Lines 144-167

**Code to REMOVE**:
```python
class _BufferProxy:
    """Mutable fallback view for buffers without writable memoryview support."""

    def __init__(self, buffer: _vertebra.Buffer, length: int) -> None:
        self._buffer = buffer
        self._length = length
        payload = buffer.read()
        self._data = bytearray(payload[:length])

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, key: int | slice) -> Any:
        return self._data[key]

    def __setitem__(self, key: int | slice, value: Any) -> None:
        self._data[key] = value
        self._buffer.write(bytes(self._data))

    def __bytes__(self) -> bytes:
        return bytes(self._data)

    def tobytes(self) -> bytes:
        return bytes(self._data)
```

- [ ] Delete entire `_BufferProxy` class (lines 144-167)

#### 1.7.2 Update `BufferView.as_memoryview()` to remove _BufferProxy fallback
**Location**: Lines 59-75

**Before**:
```python
def as_memoryview(self) -> memoryview:
    if self._buffer is None:
        msg = "buffer released"
        raise ChannelError(msg)
    try:
        view = memoryview(cast("Any", self._buffer))[: self._length]
    except TypeError:
        view_proxy = _BufferProxy(self._buffer, self._length)  # REMOVE THIS
        self._view = view_proxy
        return cast("memoryview", view_proxy)
    if view.readonly:
        view_proxy = _BufferProxy(self._buffer, self._length)  # REMOVE THIS
        self._view = view_proxy
        return cast("memoryview", view_proxy)
    self._view = view
    return view
```

**After**:
```python
def as_memoryview(self) -> memoryview:
    if self._buffer is None:
        msg = "buffer released"
        raise ChannelError(msg)
    view = memoryview(cast("Any", self._buffer))[: self._length]
    if view.readonly:
        msg = "buffer is read-only, cannot create writable memoryview"
        raise ChannelError(msg)
    self._view = view
    return view
```

- [ ] Remove `_BufferProxy` imports/references from lines 67-73

#### 1.7.3 Ensure both sync and async paths use memoryview
- [ ] Verify `BufferView.as_memoryview()` used in sync methods
- [ ] Verify same in async methods

**Verification**:
```bash
uv run pytest src/py/tests/unit/ipc/test_channel.py -v
```

---

### Task 1.8: Phase 1 Integration Verification

- [ ] Run `make test` - Python tests pass
- [ ] Run `make js-test` - TypeScript tests pass
- [ ] Run `make rs-test` - Rust tests pass
- [ ] Run `make lint-all` - No lint errors

**Profiling Verification**:
```bash
# Python: Verify zero allocations in recv() hot path
uv run python -c "
import tracemalloc
from vertebra.ipc.channel import ZeroCopyChannel
tracemalloc.start()
# ... test recv() loop
current, peak = tracemalloc.get_traced_memory()
print(f'Peak memory: {peak} bytes')
"

# TypeScript: Verify buffer pool usage
cd src/ts && bun run benchmarks/allocation-check.ts
```

---

## PHASE 2: Event-Driven Wakeups (Sub-10μs Target)

### Task 2.1: REMOVE Polling from Bun BrokerServer (CRITICAL)

**File**: `src/ts/src/broker/server.ts`

#### 2.1.1 REMOVE `setInterval(poll, 1)` (DEPRECATED)
**Location**: Lines 53-55

**Code to REMOVE**:
```typescript
this.timer = setInterval(() => {
  this.poll();
}, 1);
```

- [ ] Delete lines 53-55 completely

#### 2.1.2 Add inherited FD event loop integration
- [ ] Read `VERTEBRA_NOTIFY_FD` from environment
- [ ] Use `Bun.file(fd).readable` for event-driven wakeup

**After**:
```typescript
start(): void {
  if (this.timer) return;

  const notifyFd = parseInt(process.env.VERTEBRA_NOTIFY_FD ?? "", 10);
  if (!isNaN(notifyFd) && notifyFd > 0) {
    // Event-driven wakeup using inherited FD
    const file = Bun.file(notifyFd);
    // Register with event loop...
  } else {
    // Fallback to setInterval only if no inherited FD
    this.timer = setInterval(() => this.poll(), 1);
  }
}
```

#### 2.1.3 Verify no CPU burn when idle
- [ ] Test: Broker with no clients uses near-zero CPU
- [ ] Test: Add performance counter for poll() calls

**Verification**:
```bash
cd src/ts && bun test src/tests/unit/broker-server.test.ts
# Verify no setInterval in code:
grep -n "setInterval" src/ts/src/broker/server.ts
# Should only show conditional fallback
```

---

### Task 2.2: Add Event-Driven Client Notification in Broker

**File**: `src/ts/src/broker/server.ts`

#### 2.2.1 Notify clients via inherited eventfd when response ready
- [ ] After sending response to ring buffer, call `notifier.notify()`
- [ ] Client's event loop wakes up immediately

#### 2.2.2 Add notification FD to client context
- [ ] Store client's notify FD in `ClientContext` type (line 19-24)

**Before**:
```typescript
type ClientContext = {
  clientId: string;
  reqRing: ShmRing;
  respRing: ShmRing;
  lastActive: number;
};
```

**After**:
```typescript
type ClientContext = {
  clientId: string;
  reqRing: ShmRing;
  respRing: ShmRing;
  notifyFd?: number;  // Inherited eventfd for wakeup
  lastActive: number;
};
```

**Verification**:
```bash
cd src/ts && bun test src/tests/integration/broker-notify.test.ts
```

---

### Task 2.3: REMOVE asyncio.sleep() from Python AsyncShmTransport (CRITICAL)

**File**: `src/py/vertebra/ipc/async_transport.py`

#### 2.3.1 Update transport to use `asyncio.add_reader()`
- [ ] Read `VERTEBRA_NOTIFY_FD` from environment
- [ ] Use `loop.add_reader(notify_fd, callback)` for wakeups

**Code pattern**:
```python
async def _wait_for_data(self) -> None:
    notify_fd = int(os.environ.get("VERTEBRA_NOTIFY_FD", "0"))
    if notify_fd > 0:
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        loop.add_reader(notify_fd, future.set_result, None)
        try:
            await future
        finally:
            loop.remove_reader(notify_fd)
        # Clear eventfd counter
        os.read(notify_fd, 8)
    else:
        await asyncio.sleep(0.001)  # Fallback only
```

#### 2.3.2 Clear eventfd after wakeup
- [ ] Read 8 bytes from eventfd to reset counter
- [ ] Prevent spurious wakeups

#### 2.3.3 Test with uvloop
- [ ] Verify same behavior with uvloop installed
- [ ] `add_reader()` should work identically

**Verification**:
```bash
uv run pytest src/py/tests/unit/ipc/test_async_transport.py -v
```

---

### Task 2.4: REMOVE `asyncio.to_thread()` from Sync Path (CRITICAL)

**File**: `src/py/vertebra/ipc/channel.py`

#### 2.4.1 REMOVE `asyncio.to_thread()` from `Channel.send()` (DEPRECATED)
**Location**: Lines 274-281

**Before**:
```python
async def send(
    self,
    procedure_id: str,
    params: dict[str, Any],
    *,
    timeout_ms: int | None = None,
    response_type: type | None = None,
    serializer: str | None = None,
) -> Any:
    return await asyncio.to_thread(
        self.send_sync,
        procedure_id,
        params,
        timeout_ms=timeout_ms,
        response_type=response_type,
        serializer=serializer,
    )
```

**After** (use native async with event-driven wakeup):
```python
async def send(
    self,
    procedure_id: str,
    params: dict[str, Any],
    *,
    timeout_ms: int | None = None,
    response_type: type | None = None,
    serializer: str | None = None,
) -> Any:
    # Prepare message (same as sync)
    handle = self._require_handle()
    # ... encoding logic ...

    # Send and wait using eventfd-based wakeup
    handle.send_request(message)
    await self._wait_for_response()
    response = handle.recv_response(self._max_payload)

    # Deserialize (same as sync)
    return serializer_impl.deserialize(response, response_type)
```

- [ ] Rewrite `send()` to avoid thread pool
- [ ] Use eventfd-based `_wait_for_response()` helper

#### 2.4.2 REMOVE `asyncio.to_thread()` from `Channel.receive()` (DEPRECATED)
**Location**: Lines 321-323

**Before**:
```python
async def receive(
    self, *, timeout_ms: int | None = None, params_type: type | None = None, serializer: str | None = None
) -> tuple[str, Any]:
    return await asyncio.to_thread(
        self.receive_sync, timeout_ms=timeout_ms, params_type=params_type, serializer=serializer
    )
```

- [ ] Rewrite to use event-driven wakeup

#### 2.4.3 REMOVE `asyncio.to_thread()` from `Channel.respond()` (DEPRECATED)
**Location**: Lines 347-349

**Before**:
```python
async def respond(
    self, procedure_id: str, result: Any, *, response_type: type | None = None, serializer: str | None = None
) -> None:
    await asyncio.to_thread(
        self.respond_sync, procedure_id, result, response_type=response_type, serializer=serializer
    )
```

- [ ] Rewrite to not use thread pool

**Verification**:
```bash
uv run pytest src/py/tests/unit/ipc/test_channel.py -v
# Verify no asyncio.to_thread in channel.py:
grep -n "asyncio.to_thread" src/py/vertebra/ipc/channel.py
# Should return empty
```

---

### Task 2.5: Optimize Sync Methods in Channel

**File**: `src/py/vertebra/ipc/channel.py`

#### 2.5.1 Use `select.select()` for sync blocking
- [ ] Replace `time.sleep(0.001)` polling with `select.select([fd], [], [], timeout)`
- [ ] Direct syscall, no thread pool overhead

**Before** (lines 247, 257, 301):
```python
time.sleep(0.001)
```

**After**:
```python
import select
# ...
ready, _, _ = select.select([notify_fd], [], [], remaining_timeout)
if not ready:
    raise ChannelError("timeout", ...)
```

#### 2.5.2 Read notify FD from environment
- [ ] `notify_fd = int(os.environ.get("VERTEBRA_NOTIFY_FD", "0"))`
- [ ] Fall back to sleep polling if no FD available

**Verification**:
```bash
uv run pytest src/py/tests/unit/ipc/test_channel.py::test_sync_methods -v
```

---

### Task 2.6: REMOVE `asyncio.sleep(0.001)` from Python Worker (CRITICAL)

**File**: `src/py/vertebra/server/worker.py`

#### 2.6.1 REMOVE polling sleep in serve loop (DEPRECATED)
**Location**: Line 232

**Before**:
```python
if not did_work:
    await asyncio.sleep(0.001)
```

**After** (use event-driven wakeup):
```python
if not did_work:
    await self._wait_for_notification()

async def _wait_for_notification(self) -> None:
    """Wait for notification on inherited eventfd."""
    notify_fd = int(os.environ.get("VERTEBRA_NOTIFY_FD", "0"))
    if notify_fd > 0:
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        loop.add_reader(notify_fd, future.set_result, None)
        try:
            await asyncio.wait_for(future, timeout=1.0)
        except asyncio.TimeoutError:
            pass
        finally:
            loop.remove_reader(notify_fd)
        # Clear eventfd
        try:
            os.read(notify_fd, 8)
        except (OSError, BlockingIOError):
            pass
    else:
        await asyncio.sleep(0.001)  # Fallback only
```

- [ ] Replace sleep with `add_reader()` on notify FD
- [ ] Handle shutdown signals properly

#### 2.6.2 Handle shutdown properly with eventfd
- [ ] Write to eventfd on shutdown to wake worker
- [ ] Check `_running` flag after wakeup

**Verification**:
```bash
uv run pytest src/py/tests/integration/server/test_asgi_worker.py -v
# Verify no unconditional asyncio.sleep in worker.py:
grep -n "asyncio.sleep" src/py/vertebra/server/worker.py
# Should only show conditional fallback
```

---

### Task 2.7: Phase 2 Latency Verification

- [ ] Run latency benchmark: verify p99 < 10μs
- [ ] Verify idle CPU usage < 0.1% per core

**Benchmark Commands**:
```bash
# Create latency benchmark script
uv run python -m vertebra.benchmarks.ipc_latency --target-p99-us=10

# Check CPU usage when idle
top -b -n 5 -d 1 | grep -E "(bun|python)"
# CPU should be near 0% when no requests
```

---

## PHASE 3: FlatBuffers Control Plane (Zero Parse Cost)

### Task 3.1: Create FlatBuffers Schema

**File**: `src/schema/ipc.fbs` (NEW FILE)

- [ ] Create `src/schema/` directory
- [ ] Define `Request` table
- [ ] Define `Response` table
- [ ] Define `ControlMessage` table
- [ ] Define `Stats` table
- [ ] Define `RequestHeader` inline struct

```flatbuffers
namespace vertebra.ipc;

struct RequestHeader {
  id: uint64;
  flags: uint32;
  payload_len: uint32;
}

table Request {
  header: RequestHeader;
  procedure_id: string;
  // payload is separate (MessagePack)
}

table Response {
  id: uint64;
  ok: bool;
  error_code: int32;
  error_message: string;
  // payload is separate (MessagePack)
}

table ControlMessage {
  kind: string;
  client_id: string;
  req_ring: string;
  resp_ring: string;
}

root_type Request;
```

**Verification**:
```bash
flatc --version  # Ensure flatc is installed
flatc -o /tmp --rust --python --ts src/schema/ipc.fbs
```

---

### Task 3.2: Generate Rust FlatBuffers Code

**File**: `src/rs/vertebra-core/src/protocol/` (NEW DIRECTORY)

#### 3.2.1 Add flatbuffers crate dependency
- [ ] Add to `src/rs/vertebra-core/Cargo.toml`:
  ```toml
  [dependencies]
  flatbuffers = "24.3"
  ```

#### 3.2.2 Add code generation to build.rs
- [ ] Create/modify `src/rs/vertebra-core/build.rs`
- [ ] Run `flatc` during build

#### 3.2.3 Create protocol module
- [ ] Create `src/rs/vertebra-core/src/protocol/mod.rs`
- [ ] Include generated code

**Verification**:
```bash
cd src/rs && cargo build --package vertebra-core
```

---

### Task 3.3: Generate Python FlatBuffers Code

**File**: `src/py/vertebra/protocol/flatbuffers/` (NEW DIRECTORY)

#### 3.3.1 Add flatbuffers dependency
- [ ] Add to `pyproject.toml`:
  ```toml
  dependencies = [
    "flatbuffers>=24.3",
  ]
  ```

#### 3.3.2 Create generation script
- [ ] Create `scripts/generate_flatbuffers.py`
- [ ] Run `flatc --python` on schema

#### 3.3.3 Create protocol module
- [ ] Create `src/py/vertebra/protocol/flatbuffers/__init__.py`

**Verification**:
```bash
uv run python -c "from vertebra.protocol.flatbuffers import Request"
```

---

### Task 3.4: Generate TypeScript FlatBuffers Code

**File**: `src/ts/src/protocol/flatbuffers/` (NEW DIRECTORY)

#### 3.4.1 Add flatbuffers dependency
- [ ] Add to `src/ts/package.json`:
  ```json
  "dependencies": {
    "flatbuffers": "^24.3.0"
  }
  ```

#### 3.4.2 Add generation to build
- [ ] Add npm script for flatc generation
- [ ] Run during `bun run build`

#### 3.4.3 Create protocol module
- [ ] Create `src/ts/src/protocol/flatbuffers/index.ts`

**Verification**:
```bash
cd src/ts && bun run build
bun -e "import { Request } from './src/protocol/flatbuffers'"
```

---

### Task 3.5: Integrate FlatBuffers Headers with MessagePack Payloads

**Files**: Multiple

#### 3.5.1 Update Rust message encoding
- [ ] Use FlatBuffers for headers
- [ ] Keep MessagePack for dynamic payloads

#### 3.5.2 Update Python message encoding
- [ ] Same hybrid approach

#### 3.5.3 Update TypeScript message encoding
- [ ] Same hybrid approach

**Verification**:
```bash
make test-all
```

---

### Task 3.6: REMOVE Dead Code from serialization.ts (DEPRECATED)

**File**: `src/ts/src/protocol/serialization.ts`

#### 3.6.1 REMOVE `frameMessage` function
**Location**: Lines 14-22

**Code to REMOVE**:
```typescript
export function frameMessage(payload: Uint8Array): Uint8Array {
  const header = new Uint8Array(4);
  const view = new DataView(header.buffer);
  view.setUint32(0, payload.length, false);
  const framed = new Uint8Array(4 + payload.length);
  framed.set(header, 0);
  framed.set(payload, 4);
  return framed;
}
```

- [ ] Delete `frameMessage` function (lines 14-22)

#### 3.6.2 REMOVE `deframeMessage` function
**Location**: Lines 24-35

**Code to REMOVE**:
```typescript
export function deframeMessage(payload: Uint8Array): Uint8Array {
  if (payload.length < 4) {
    throw new Error("Frame too small");
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const size = view.getUint32(0, false);
  const body = payload.subarray(4);
  if (body.length !== size) {
    throw new Error("Frame size mismatch");
  }
  return body;
}
```

- [ ] Delete `deframeMessage` function (lines 24-35)

#### 3.6.3 Update test file
**File**: `src/ts/tests/unit/serialization.test.ts`

- [ ] Remove tests for `frameMessage` and `deframeMessage` (lines 4-6, 20-21)
- [ ] Or delete entire test file if only testing removed functions

**Verification**:
```bash
cd src/ts && bun test
# Verify removed:
grep -n "frameMessage\|deframeMessage" src/ts/src/protocol/serialization.ts
# Should return empty
```

---

### Task 3.7: Phase 3 Verification

- [ ] Run `make test-all` - all tests pass
- [ ] Verify p99 latency still < 10μs
- [ ] Verify CPU usage reduced (less parsing work)

---

## PHASE 4: Request Multiplexing + Polish

### Task 4.1: Add Correlation ID Tracking in TypeScript

**File**: `src/ts/src/ipc/client.ts`

#### 4.1.1 Replace single pending with Map
- [ ] Change from `pending: DeferredPromise` to `pending: Map<number, DeferredPromise>`
- [ ] Generate unique correlation IDs

#### 4.1.2 Allow 64 concurrent in-flight requests
- [ ] Configure max concurrent requests
- [ ] Handle backpressure when limit reached

#### 4.1.3 Handle out-of-order responses
- [ ] Match responses by correlation ID
- [ ] Resolve correct promise

**Verification**:
```bash
cd src/ts && bun test src/tests/unit/ipc-client.test.ts
```

---

### Task 4.2: Add Multiplexing Support in Python

**File**: `src/py/vertebra/resources/broker.py`

#### 4.2.1 Thread-safe pending map for sync
- [ ] Use `threading.Lock` protected dict
- [ ] Or use `concurrent.futures.Future`

#### 4.2.2 asyncio.Future-based tracking for async
- [ ] Use `dict[int, asyncio.Future]` for pending requests
- [ ] Resolve futures when responses arrive

#### 4.2.3 Both paths support concurrent requests
- [ ] Test with 64 concurrent requests

**Verification**:
```bash
uv run pytest src/py/tests/unit/resources/test_broker.py -v
```

---

### Task 4.3: CONSIDER nanoarrow Removal (Optional)

**File**: `src/py/vertebra/resources/arrow.py`

#### 4.3.1 Evaluate nanoarrow usage
- [ ] Check if nanoarrow is actually used in production
- [ ] Lines 78-79, 101-155 reference nanoarrow

#### 4.3.2 If removing, make pyarrow required
- [ ] Update `pyproject.toml` to make pyarrow non-optional
- [ ] Remove nanoarrow fallback code

**Note**: This is marked optional - only proceed if team decides to simplify.

---

### Task 4.4: Final Benchmark Suite

#### 4.4.1 Latency benchmark
```bash
uv run python -m vertebra.benchmarks.ipc_latency --target-p99-us=10
```
- [ ] p50 < 5μs
- [ ] p99 < 10μs
- [ ] p999 < 50μs

#### 4.4.2 Throughput benchmark
```bash
uv run python -m vertebra.benchmarks.ipc_throughput --target-msgs-sec=1000000
```
- [ ] > 1M msg/sec per connection

#### 4.4.3 Allocation benchmark
```bash
uv run python -m vertebra.benchmarks.ipc_allocations --max-allocs=0
```
- [ ] Zero allocations in hot path

#### 4.4.4 Sync/Async parity benchmark
```bash
uv run python -m vertebra.benchmarks.ipc_sync_async_parity
```
- [ ] Both achieve same latency targets

---

### Task 4.5: Final Integration Verification

- [ ] `make test` passes
- [ ] `make js-test` passes
- [ ] `make rs-test` passes
- [ ] `make lint-all` passes
- [ ] All deprecation removals verified (see checklist below)

---

## DEPRECATION REMOVAL CHECKLIST

### Code Removals (Must Complete)

| Item | File | Lines | Status |
|------|------|-------|--------|
| `EFD_CLOEXEC` flag | `src/rs/vertebra-core/src/notify.rs` | 61 | [ ] |
| `setInterval(poll, 1)` | `src/ts/src/broker/server.ts` | 53-55 | [ ] |
| `asyncio.sleep(0.001)` | `src/py/vertebra/server/worker.py` | 232 | [ ] |
| `asyncio.to_thread()` #1 | `src/py/vertebra/ipc/channel.py` | 274-281 | [ ] |
| `asyncio.to_thread()` #2 | `src/py/vertebra/ipc/channel.py` | 321-323 | [ ] |
| `asyncio.to_thread()` #3 | `src/py/vertebra/ipc/channel.py` | 347-349 | [ ] |
| `_BufferProxy` class | `src/py/vertebra/ipc/channel.py` | 144-167 | [ ] |
| `_BufferProxy` usage #1 | `src/py/vertebra/ipc/channel.py` | 67-69 | [ ] |
| `_BufferProxy` usage #2 | `src/py/vertebra/ipc/channel.py` | 71-73 | [ ] |
| `frameMessage` | `src/ts/src/protocol/serialization.ts` | 14-22 | [ ] |
| `deframeMessage` | `src/ts/src/protocol/serialization.ts` | 24-35 | [ ] |
| `time.sleep(0.001)` #1 | `src/py/vertebra/ipc/channel.py` | 247 | [ ] |
| `time.sleep(0.001)` #2 | `src/py/vertebra/ipc/channel.py` | 257 | [ ] |
| `time.sleep(0.001)` #3 | `src/py/vertebra/ipc/channel.py` | 301 | [ ] |

### Verification Commands

```bash
# Verify all deprecated code is removed:
grep -rn "EFD_CLOEXEC" src/rs/vertebra-core/src/notify.rs
grep -n "setInterval.*poll" src/ts/src/broker/server.ts
grep -n "asyncio.sleep(0.001)" src/py/vertebra/server/worker.py
grep -n "asyncio.to_thread" src/py/vertebra/ipc/channel.py
grep -n "_BufferProxy" src/py/vertebra/ipc/channel.py
grep -n "frameMessage\|deframeMessage" src/ts/src/protocol/serialization.ts
grep -n "time.sleep(0.001)" src/py/vertebra/ipc/channel.py

# All commands above should return empty (no matches)
```

---

## VERIFICATION MATRIX

### Phase 1 Complete When:
- [ ] `make test-all` passes
- [ ] Zero allocations in `recv()` hot path (profiler verified)
- [ ] FD inheritance works across fork (strace verified)
- [ ] `_BufferProxy` class removed
- [ ] `EFD_CLOEXEC` not present in new `InheritableNotifier`

### Phase 2 Complete When:
- [ ] No `setInterval` in server.ts (except conditional fallback)
- [ ] No `asyncio.sleep` in worker.py (except conditional fallback)
- [ ] No `asyncio.to_thread` in channel.py
- [ ] No `time.sleep(0.001)` polling in channel.py
- [ ] p99 latency < 10μs
- [ ] Idle CPU < 0.1%

### Phase 3 Complete When:
- [ ] FlatBuffers schema defined in `src/schema/ipc.fbs`
- [ ] Code generated for Rust, Python, TypeScript
- [ ] Control plane uses FlatBuffers
- [ ] `frameMessage`/`deframeMessage` removed

### Phase 4 Complete When:
- [ ] 64+ concurrent requests supported
- [ ] Throughput > 1M msg/sec
- [ ] Sync and async achieve same latency
- [ ] All deprecation checklist items complete

---

## FINAL SIGN-OFF

Before marking implementation complete:

1. [ ] All 52 tasks checked off
2. [ ] All 14 deprecation items removed
3. [ ] All 4 phase verification matrices green
4. [ ] `make test-all` passes
5. [ ] `make lint-all` passes
6. [ ] Benchmark suite meets targets
7. [ ] Documentation updated (if applicable)
