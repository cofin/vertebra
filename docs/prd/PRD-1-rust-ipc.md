# PRD-1: Rust Core IPC Layer

**Version**: 1.0
**Status**: Complete
**Created**: 2026-01-17
**Author**: Claude Code
**Dependencies**: PRD-0 (Foundation)

---

## Executive Summary

### Problem Statement

Vertebra is a Python-first, high-performance application backbone designed for data-intensive workloads. As the system scales, efficient inter-process communication (IPC) becomes critical for:

- **Data parallelism**: Distributing workloads across multiple Python processes
- **Service isolation**: Separating concerns while maintaining high throughput
- **Zero-copy data exchange**: Minimizing latency and memory overhead for large datasets

Current Python IPC mechanisms (multiprocessing, socket-based) introduce unacceptable latency (milliseconds) and require data serialization, creating performance bottlenecks. For high-performance data processing pipelines, microsecond-level latency and zero-copy semantics are essential.

### Solution Overview

PRD-1 introduces a **Rust-based core IPC layer** that provides:

1. **POSIX shared memory management** for zero-copy data sharing
2. **Lock-free channels** (SPSC/MPMC) for message passing
3. **Pre-allocated buffer pools** for memory efficiency
4. **Cross-process synchronization primitives** (futex-based)
5. **FFI surfaces** exposed via `vertebra-py` (PyO3) and `vertebra-js` (napi-rs), with optional C ABI for future bindings
6. **Apache Arrow IPC support** for structured data exchange (optional, gated by benchmarks)

**Key Benefits**:
- Sub-microsecond latency (< 1μs target) for small messages
- Memory safety guaranteed by Rust's ownership system
- Verified via Miri for undefined behavior detection
- Cross-platform design with Linux-first implementation (macOS/Windows parity target)
- Python integration through PyO3 (and optional C ABI where needed)

### Success Criteria

1. **Performance**: < 1μs round-trip latency for messages < 1KB (stretch), with baseline measurements documented
2. **Safety**: All Miri tests pass (no undefined behavior)
3. **Coverage**: 90%+ test coverage across all modules
4. **Reliability**: Zero data loss under normal operation
5. **Integration**: Seamless Python/Rust integration with comprehensive examples

---

## Background and Context

### Vertebra Project Overview

Vertebra is a modern application backbone combining Python's productivity with high-performance components for data processing. The project structure follows a multi-language monorepo pattern:

```
src/
├── py/vertebra/         # Python package
├── ts/                  # TypeScript/Bun components
└── rs/                  # Rust components (new)
```

The Python ecosystem (`pyproject.toml`, `Makefile`) is established with modern tooling (uv, ruff, pytest). The Rust ecosystem will integrate into this existing infrastructure.

### Why Rust for IPC?

Rust is uniquely positioned for high-performance IPC:

1. **Memory Safety**: Ownership system prevents data races and use-after-free
2. **Zero-Cost Abstractions**: Performance equivalent to C with higher-level APIs
3. **FFI Stability**: PyO3 + optional abi3, and napi-rs for Node/Bun compatibility
4. **Ecosystem**: Rich libraries for concurrency (crossbeam), I/O (rustix), and data (arrow-rs)
5. **Verification**: Miri enables undefined behavior detection in unsafe code

### Cross-Platform Approach (Linux-first, MVP parity target)

PRD-1 targets Linux as the primary platform for initial development, while keeping the IPC layer **cross-platform-ready** for macOS and Windows at MVP when feasible.

- **Linux**: POSIX shared memory + futex-based synchronization
- **macOS**: POSIX shared memory + pthread `pshared` primitives
- **Windows**: Named shared memory + named mutex/condvar equivalents

If cross-platform parity slips, the design **must not** introduce Linux-only assumptions that block later support. Platform-specific implementations live behind a unified trait/interface layer.

### Relationship to PRD-0

PRD-0 (Foundation) establishes the base project structure, including:
- Python package scaffolding
- Build system configuration
- Testing and CI infrastructure
- Documentation standards
- **Cross-PRD Architectural Decisions** (serialization strategy, dependency versions, platform IPC backends)

PRD-1 builds atop this foundation by adding the Rust subsystem, which integrates with existing Python components while maintaining clear architectural boundaries.

> **Note**: See PRD-0 "Cross-PRD Architectural Decisions" for canonical decisions on:
> - Python 3.12+ baseline
> - Serialization: bincode for internal Rust IPC (this PRD), msgspec for Python codec
> - thiserror 2.0 for error handling
> - Cross-platform IPC backends (shm_open + UDS on Linux/macOS, CreateFileMapping + Named Pipes on Windows)

### Assumptions and Constraints

- **Cross-platform target**: Linux + macOS + Windows are MVP targets if feasible; Linux is the initial implementation path.
- **Platform abstraction**: shared memory and sync primitives must be behind OS-specific backends.
- **Python 3.12+** baseline (aligned with `pyproject.toml`).
- **FFI surface** lives in `vertebra-py` / `vertebra-js`; **optional C ABI** is in-scope only if required for best native compatibility.
- **Performance targets** require measured baselines; stretch goals need explicit sign-off.

### Resolved OS-API Shim Decisions (Max Performance)

**Shared memory backing**
- **Linux**: `memfd_create` fast-path + seals after sizing; `shm_open` fallback.
- **macOS**: `shm_open` + `ftruncate` + `mmap`, `shm_unlink` immediately after handle handoff.
- **Windows**: `CreateFileMappingW` (unnamed, pagefile-backed) + `MapViewOfFile`, pass handle via `DuplicateHandle`.
- **Discovery**: handle/fd passing over control channel (UDS on Unix; named pipe or inherited handle on Windows). Avoid named mappings except for fallback rendezvous.

**Cross-process sync**
- **Linux**: futex wait/wake on shared `AtomicU32`.
- **macOS**: `ulock_wait/ulock_wake` fast-path (**opt-in feature `macos-ulock`**). Default is a spin + nanosleep fallback until a shared pthread implementation lands.
- **Windows**: `WaitOnAddress` / `WakeByAddress*`.
- **Strategy**: lock-free data path + short spin before OS wait.
- **Timeouts**: `wait_on_address_timeout` uses OS-native timed waits (futex/ulock/WaitOnAddress) with bounded spin fallback on macOS when ulock is disabled.

**Lifecycle & crash recovery**
- **Header protocol**: `magic`, `abi_version`, `total_size`, `state`, `generation`, `creator_pid`, `creator_epoch`, optional heartbeat.
- **Recovery**: detect stale/owner death and reset ring + bump generation (protocol-based, not robust mutex-based).

**Layout contract**
- Versioned C-ABI header with cacheline-aligned hot atomics (128B on Apple Silicon macOS, 64B elsewhere).
- Power-of-two ring, **fixed-slot** sizing (size classes negotiated in header).
- Slot prefix: `len + flags`, payload aligned to cacheline boundaries.

---

## Requirements

### Functional Requirements

#### FR-1: Shared Memory Management (shm.rs)

**Description**: Provide safe abstractions over POSIX shared memory for zero-copy data exchange between processes.

**Capabilities**:
- Create named shared memory regions with specified size
- Open existing shared memory regions by name
- Memory-map regions into process address space (read/write)
- Automatic cleanup on process exit (RAII pattern)
- Safe destruction with `shm_unlink()`

**API Surface**:
```rust
pub struct ShmRegion { /* ... */ }

impl ShmRegion {
    pub fn create(size: usize) -> Result<Self, ShmError>;
    pub fn create_named(name: &str, size: usize) -> Result<Self, ShmError>;
    pub fn open_named(name: &str, size: usize) -> Result<Self, ShmError>;
    pub fn as_slice(&self) -> &[u8];
    pub fn as_mut_slice(&mut self) -> &mut [u8];
    pub fn size(&self) -> usize;
}
```

**Constraints**:
- Names must follow POSIX requirements (start with `/`, max 255 chars)
- Size must be page-aligned (4KB on x86_64)
- Permissions: 0600 (owner read/write only)
- Windows backend will use named shared memory conventions (e.g., `Local\\`/`Global\\`)

#### FR-2: Lock-Free Channels (channel.rs)

**Description**: Implement high-performance message passing channels using lock-free ring buffers.

**Capabilities**:
- **SPSC (Single-Producer Single-Consumer)**: Optimized for 1-to-1 communication
  - Bounded channels (fixed capacity)
  - Cache-padded atomics to prevent false sharing
  - Relaxed memory ordering for performance

- **MPMC (Multi-Producer Multi-Consumer)**: Generalized for N-to-M communication
  - Bounded channels with backpressure
  - ABA problem mitigation (epoch-based reclamation)
  - SeqCst memory ordering for correctness

**API Surface**:
```rust
pub struct SpscChannel<T> { /* ... */ }

impl<T> SpscChannel<T> {
    pub fn new(capacity: usize) -> (Sender<T>, Receiver<T>);
}

impl<T> Sender<T> {
    pub fn send(&mut self, value: T) -> Result<(), SendError<T>>;
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>>;
}

impl<T> Receiver<T> {
    pub fn recv(&mut self) -> Result<T, RecvError>;
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>;
}

pub struct MpmcChannel<T> { /* ... */ }
// Similar API with shared ownership (Arc)
```

**Constraints**:
- `T` must be `Send` for cross-thread transfer
- Bounded channels: fixed capacity at creation
- No dynamic resizing (prevents reallocation during operation)

#### FR-3: Buffer Pool (buffer.rs)

**Description**: Pre-allocated buffer pool for zero-copy buffer management with reference counting.

**Capabilities**:
- Pre-allocate fixed-size buffers on creation
- Acquire buffers with O(1) complexity
- Release buffers back to pool automatically (Drop trait)
- Slice-based views for zero-copy access
- Thread-safe sharing via Arc

**API Surface**:
```rust
pub struct BufferPool { /* ... */ }

impl BufferPool {
    pub fn new(buffer_size: usize, count: usize) -> Arc<Self>;
    pub fn acquire(&self) -> Result<Buffer, AcquireError>;
    pub fn available(&self) -> usize;
}

pub struct Buffer { /* ... */ }

impl Buffer {
    pub fn as_slice(&self) -> &[u8];
    pub fn as_mut_slice(&mut self) -> &mut [u8];
    pub fn len(&self) -> usize;
}
```

**Constraints**:
- Fixed buffer size and count at creation
- No dynamic expansion (predictable memory usage)
- Maximum pool size: 4GB (2^32 bytes)

#### FR-4: Synchronization Primitives (sync.rs)

**Description**: Cross-process synchronization primitives based on Linux futexes.

**Capabilities**:
- **SharedMutex**: Cross-process mutual exclusion
- **SharedRwLock**: Cross-process read-write lock
- **SharedCondvar**: Cross-process condition variable
- Futex-based implementation (without FUTEX_PRIVATE_FLAG)
- Fallback to file-based locking if futex unavailable

**API Surface**:
```rust
pub struct SharedMutex<T> { /* ... */ }

impl<T> SharedMutex<T> {
    pub fn new(value: T) -> Self;
    pub fn lock(&self) -> Result<MutexGuard<T>, LockError>;
    pub fn try_lock(&self) -> Result<MutexGuard<T>, TryLockError>;
}

pub struct SharedRwLock<T> { /* ... */ }
pub struct SharedCondvar { /* ... */ }
```

**Constraints**:
- Linux kernel 4.18+ required for futex-based implementation
- macOS/Windows use platform-specific primitives behind the same API
- Must reside in shared memory for cross-process access
- Alignment: 4-byte (AtomicU32) or 8-byte (AtomicU64)

#### FR-5: FFI Surfaces (vertebra-py / vertebra-js)

**Description**: Expose IPC primitives to Python and Bun/Node via native bindings. A pure C ABI layer is optional for future language targets.

**Capabilities**:
- PyO3 module exposing IPC types for Python (managed lifetimes, error mapping)
- napi-rs module exposing IPC functions for Bun/Node
- Thread-safe operations and explicit resource lifecycles
- No panics across FFI boundaries (errors mapped to language-native exceptions)

**API Surface** (Rust binding entry points):
```rust
// vertebra-py/src/lib.rs (PyO3)
#[pymodule]
fn _vertebra(py: Python, m: &PyModule) -> PyResult<()> { /* ... */ }

// vertebra-js/src/lib.rs (napi-rs)
#[napi]
pub fn shm_create(name: String, size: u64) -> Result<JsObject>;
```

**Constraints**:
- Optional C ABI is **in-scope only if required** for best native compatibility or additional language targets.
- Error mapping must be deterministic and test-covered.

#### C ABI Decision Checklist

Use this checklist at the end of Phase 6 (bindings) to decide whether a C ABI shim is required.

**Triggers (any true → evaluate C ABI):**
1. PyO3 or napi-rs cannot meet **binary compatibility** requirements for target platforms.
2. We need a **stable ABI** for distribution across multiple Python versions/environments where `abi3` is insufficient.
3. Another language target (non‑Python/JS) is required for MVP.
4. Performance profiling shows binding overhead dominates end‑to‑end latency and cannot be reduced in PyO3/napi‑rs.

**Go/No‑Go Criteria:**
- **GO** if at least one trigger is true **and** the minimal C ABI surface can be limited to core IPC primitives with no API explosion.
- **NO‑GO** if PyO3/napi‑rs meet compatibility and performance targets, or if C ABI would delay MVP parity.

**Decision Owner**: Tech Lead (with input from bindings + platform owners).

#### FR-6: Apache Arrow IPC Integration

**Description**: Support for Apache Arrow IPC format to exchange structured data.

**Capabilities**:
- Serialize/deserialize Arrow RecordBatch to/from shared memory
- Zero-copy reading via memory-mapped IPC files
- C data interface for Python PyArrow integration
- 64-byte alignment for SIMD optimization

**API Surface**:
```rust
pub fn write_arrow_ipc(
    buffer: &mut [u8],
    batch: &RecordBatch,
) -> Result<usize, ArrowError>;

pub fn read_arrow_ipc(
    buffer: &[u8],
) -> Result<RecordBatch, ArrowError>;
```

**Constraints**:
- Arrow schema must be shared separately (out-of-band)
- Buffer size must accommodate entire RecordBatch
- Feature is **gated by benchmarks** and may ship after core IPC APIs
- No streaming support in Phase 1 (future enhancement)

### Non-Functional Requirements

#### NFR-1: Performance

- **Latency**: < 1μs round-trip for messages < 1KB (SPSC channel)
- **Throughput**: > 1M messages/second (SPSC, single core)
- **Memory Overhead**: < 10% of data size
- **CPU Usage**: Event-driven (no busy-waiting in hot paths)

**Measurement**:
- Criterion benchmarks with 10,000+ iterations
- Profiling via perf + flamegraph
- Tail latency tracking (P50, P90, P99, P99.9)

#### NFR-2: Scalability

- **Channels**: Support 1-1000 concurrent channels per process
- **Shared Memory**: Up to 4GB per region
- **Buffer Pool**: Configurable from MB to GB range
- **Processes**: 2-16 processes per deployment

#### NFR-3: Reliability

- **Data Integrity**: No message loss under normal operation
- **Crash Recovery**: Detect and cleanup stale shared memory regions
- **Graceful Degradation**: Handle resource exhaustion without panics
- **Automatic Cleanup**: RAII patterns ensure resource release

#### NFR-4: Compatibility

- **Platform**: Linux (4.18+), macOS, Windows (x86_64, aarch64 where supported)
- **Python**: 3.12+ (matching pyproject.toml)
- **Rust**: 1.75+ (stable), nightly for Miri
- **FFI**: PyO3 + napi-rs, optional abi3 for Python stability; optional C ABI if required

#### NFR-5: Security

- **Memory Safety**: No undefined behavior (Miri validated)
- **Process Isolation**: No shared credentials or secrets
- **Permissions**: Shared memory regions with 0600 mode
- **Bounds Checking**: All buffer access validated

#### NFR-6: Maintainability

- **Code Coverage**: 90%+ (unit + integration tests)
- **Documentation**: Rustdoc for all public APIs
- **Examples**: End-to-end Python integration examples
- **Error Messages**: Descriptive, actionable error messages

---

## Technical Design

### Architecture Overview

The Rust Core IPC Layer follows a layered architecture:

```
┌─────────────────────────────────────────┐
│     Python/Bun Application Layers       │
│ (vertebra-py / vertebra-js bindings)    │
└─────────────────────────────────────────┘
                    ▲
                    │ PyO3 / napi-rs
                    ▼
┌─────────────────────────────────────────┐
│   vertebra-py / vertebra-js bindings    │
│   Error mapping, resource lifecycles    │
└─────────────────────────────────────────┘
                    ▲
                    │ Internal Rust API
                    ▼
┌─────────────────────────────────────────┐
│         vertebra-core (Rust lib)        │
│  ┌─────────┬──────────┬────────┬──────┐ │
│  │  shm.rs │channel.rs│buffer  │sync  │ │
│  │         │          │.rs     │.rs   │ │
│  └─────────┴──────────┴────────┴──────┘ │
└─────────────────────────────────────────┘
                    ▲
                    │ System calls
                    ▼
┌─────────────────────────────────────────┐
│      Linux Kernel (POSIX APIs)          │
│  shm_open, mmap, futex, ...             │
└─────────────────────────────────────────┘
```

**Component Interaction**:
1. Python/Bun applications import vertebra bindings
2. Binding layers validate inputs, call vertebra-core APIs
3. Core modules use rustix for safe POSIX access
4. Kernel provides shared memory and synchronization

### Component Design

#### Shared Memory Management (shm.rs)

**Implementation Strategy**:

1. **ShmRegion struct**:
   - Wraps file descriptor from `shm_open()`
   - Stores mmap pointer and size
   - Implements Drop for automatic cleanup

2. **POSIX Workflow**:
   ```rust
   // Create region
   let fd = rustix::shm::open(name, OFlags::CREAT | OFlags::RDWR, Mode::from_bits(0o600))?;
   rustix::fs::ftruncate(&fd, size)?;
   let ptr = rustix::mm::mmap(
       null_mut(),
       size,
       ProtFlags::READ | ProtFlags::WRITE,
       MapFlags::SHARED,
       &fd,
       0,
   )?;
   rustix::io::close(fd); // Close fd after mmap
   ```

3. **Safety Guarantees**:
   - Mutable access requires exclusive ownership (Rust borrow checker)
   - Drop trait ensures `munmap()` and `shm_unlink()` called
   - Named regions prevent accidental collisions

**Memory Layout**:
```
┌───────────────────────────────────────┐
│  ShmRegion Header (metadata)          │ 64 bytes
├───────────────────────────────────────┤
│  User Data (raw bytes)                │ size - 64
└───────────────────────────────────────┘
```

Header includes:
- Magic number (validation)
- Version number (compatibility)
- Size (sanity check)
- Checksum (integrity)

#### Lock-Free Channels (channel.rs)

**SPSC Implementation**:

Based on cache-friendly ring buffer pattern from DEV Community research:

```rust
struct SpscRingBuffer<T> {
    buffer: Vec<T>,
    capacity: usize,
    head: CachePadded<AtomicUsize>,  // Consumer index
    tail: CachePadded<AtomicUsize>,  // Producer index
}
```

**Key Optimizations**:
1. **Cache Padding**: Prevents false sharing (head/tail on different cache lines)
2. **Relaxed Ordering**: Producer writes tail with `Ordering::Release`, consumer reads with `Acquire`
3. **Power-of-2 Capacity**: Enables fast modulo via bitwise AND

**MPMC Implementation**:

Requires stronger synchronization:
```rust
struct MpmcRingBuffer<T> {
    buffer: Vec<AtomicU64>, // Stores (value, epoch) pairs
    capacity: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}
```

**ABA Problem Mitigation**:
- Epoch-based versioning (upper 32 bits of AtomicU64)
- CAS operations validate epoch before swap
- Prevents spurious success when value changes A→B→A

**Backpressure Handling**:
- `send()` blocks when channel full (via futex wait)
- `try_send()` returns error immediately
- Configurable timeout variants

#### Buffer Pool (buffer.rs)

**Design Pattern**:

Inspired by buffer_sv2 crate:

```rust
pub struct BufferPool {
    buffers: Vec<u8>,           // Contiguous backing store
    buffer_size: usize,
    free_list: Mutex<Vec<usize>>, // Indices of free buffers
}

pub struct Buffer {
    pool: Arc<BufferPool>,
    offset: usize,
    size: usize,
}
```

**Allocation Algorithm**:
1. Lock free_list mutex
2. Pop index from free_list
3. Calculate offset: `index * buffer_size`
4. Return Buffer with Arc reference to pool

**Deallocation** (Drop trait):
1. Lock free_list mutex
2. Push index back to free_list
3. No actual memory free (reused for next allocation)

**Zero-Copy Pattern**:
```rust
impl Buffer {
    pub fn as_slice(&self) -> &[u8] {
        let start = self.offset;
        let end = start + self.size;
        &self.pool.buffers[start..end] // Slice, not copy
    }
}
```

**Thread Safety**:
- `Arc<BufferPool>` allows shared ownership across threads
- Mutex protects free_list modifications
- Buffer slices are non-overlapping (enforced by offset calculation)

#### Synchronization Primitives (sync.rs)

**Cross-Process Futex Wrapper**:

Critical modification: **omit FUTEX_PRIVATE_FLAG** for cross-process support.

```rust
pub struct SharedMutex<T> {
    futex: AtomicU32,  // 0 = unlocked, 1 = locked
    value: UnsafeCell<T>,
}

impl<T> SharedMutex<T> {
    pub fn lock(&self) -> MutexGuard<T> {
        loop {
            // Try to acquire lock
            if self.futex.compare_exchange(
                0, 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ).is_ok() {
                return MutexGuard { mutex: self };
            }

            // Wait on futex (cross-process)
            unsafe {
                rustix::thread::futex(
                    &self.futex as *const AtomicU32 as *mut u32,
                    FutexOperation::Wait,
                    1,  // Expected value
                    None, // No timeout
                );
            }
        }
    }
}
```

**Fallback Strategy**:

If futex operations fail (e.g., on older kernels):
1. Detect futex unavailability at runtime
2. Fall back to file-based locking (flock)
3. Log warning about performance degradation
4. Continue operation (correctness preserved)

#### FFI Surfaces (vertebra-py / vertebra-js)

**Binding Strategy**:
- PyO3 module exposes IPC types to Python (`vertebra._vertebra`)
- napi-rs module exposes IPC primitives to Bun/Node
- Binding layers handle error mapping and resource lifecycles

**Design Notes**:
- No panics across FFI boundaries
- Deterministic error conversion (Rust error → Python exception / JS error)
- Optional C ABI is deferred unless the decision checklist triggers it

#### Apache Arrow IPC Integration

**Phase 1: Serialization Support**:

```rust
use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;

pub fn write_arrow_ipc(
    buffer: &mut [u8],
    batch: &RecordBatch,
) -> Result<usize, ArrowError> {
    let mut cursor = Cursor::new(buffer);
    let mut writer = StreamWriter::try_new(&mut cursor, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(cursor.position() as usize)
}
```

**Zero-Copy Reading**:
```rust
use arrow::ipc::reader::StreamReader;

pub fn read_arrow_ipc(buffer: &[u8]) -> Result<RecordBatch, ArrowError> {
    let cursor = Cursor::new(buffer);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader.next().ok_or(ArrowError::IoError("No batch".into()))?
}
```

**Alignment Considerations**:
- Arrow requires 64-byte alignment for optimal SIMD
- Use `posix_memalign` or page-aligned shared memory
- Static assertions validate alignment at compile time

**Future Enhancement (Phase 2)**:
- Direct RecordBatch in shared memory (avoid serialization)
- C data interface for PyArrow zero-copy access
- Schema registry for dynamic type discovery

### Build System Integration

**Cargo Workspace Structure**:

```toml
# Cargo.toml (workspace root)
[workspace]
resolver = "2"
members = ["vertebra-core", "vertebra-py", "vertebra-js"]

[workspace.dependencies]
rustix = { version = "0.38", features = ["shm", "mm", "thread"] }
arrow = "53.0"
crossbeam-utils = "0.8"
libc = "0.2"
thiserror = "2.0"

[workspace.dependencies.dev]
criterion = "0.5"
proptest = "1.6"
```

```toml
# vertebra-core/Cargo.toml
[package]
name = "vertebra-core"
version = "0.1.0"
edition = "2021"

[dependencies]
rustix = { workspace = true }
arrow = { workspace = true }
crossbeam-utils = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
criterion = { workspace = true }
proptest = { workspace = true }

[lib]
crate-type = ["rlib"]
```

Binding crates (`vertebra-py`, `vertebra-js`) are defined in PRD-0 and consume `vertebra-core` via workspace dependencies.

**Makefile Integration**:

Add Rust targets to existing Makefile:

```makefile
.PHONY: rust-build
rust-build:                                        ## Build Rust components
	@echo "${INFO} Building Rust components... 🦀"
	@cd src/rs && cargo build --release
	@echo "${OK} Rust build complete"

.PHONY: rust-test
rust-test:                                         ## Run Rust tests
	@echo "${INFO} Running Rust tests... 🧪"
	@cd src/rs && cargo test
	@echo "${OK} Rust tests passed ✨"

.PHONY: rust-miri
rust-miri:                                         ## Run Miri on Rust code
	@echo "${INFO} Running Miri UB detection... 🔍"
	@cd src/rs && cargo +nightly miri test
	@echo "${OK} Miri checks passed ✨"

.PHONY: rust-bench
rust-bench:                                        ## Run Rust benchmarks
	@echo "${INFO} Running benchmarks... 📊"
	@cd src/rs && cargo bench
	@echo "${OK} Benchmarks complete"
```

**CI Pipeline**:
- Build Rust components in CI
- Run tests + Miri checks
- Generate coverage reports (cargo-tarpaulin)
- Benchmark regression detection (Criterion)

---

## Implementation Plan

### Phase 1: Foundation and Build System (Week 1)

**Deliverables**:
- Cargo workspace with `vertebra-core` and binding crates (`vertebra-py`, `vertebra-js`) wired to it
- CI pipeline (GitHub Actions or equivalent)
- Basic project structure (lib.rs, module stubs)
- Makefile integration for Rust commands
- Documentation setup (rustdoc configuration)

**Tasks**:
1. Create `src/rs/Cargo.toml` workspace manifest
2. Initialize `vertebra-core` and ensure binding crates (`vertebra-py`, `vertebra-js`) depend on it (see PRD-0)
3. Configure workspace dependencies (rustix, arrow-rs, etc.)
4. Set up CI pipeline with Rust toolchain
5. Add Makefile targets (`rust-build`, `rust-test`, etc.)
6. Create skeleton modules (shm.rs, channel.rs, buffer.rs, sync.rs)
7. Document build process in README

**Success Criteria**:
- `make rust-build` succeeds
- `make rust-test` runs (even if no tests yet)
- CI pipeline validates Rust code

### Phase 2: Shared Memory (Week 1-2)

**Deliverables**:
- Complete ShmRegion implementation
- Unit tests with 90%+ coverage
- Miri validation passing
- Multi-process integration tests

**Tasks**:
1. Implement `ShmRegion::create()` using rustix::shm
2. Implement `ShmRegion::open()` for existing regions
3. Add RAII cleanup (Drop trait with munmap/shm_unlink)
4. Implement safe slice access (as_slice/as_mut_slice)
5. Write unit tests (create, open, write, read, destroy)
6. Add property-based tests (proptest)
7. Validate with Miri (`cargo +nightly miri test`)
8. Create multi-process test (spawn child, share data)

**Success Criteria**:
- All tests pass (unit + integration)
- Miri reports no UB
- Coverage ≥ 90%
- Multi-process test validates data sharing

### Phase 3: Lock-Free Channels (Week 2-3)

**Deliverables**:
- SPSC channel implementation
- MPMC channel implementation
- Bounded and unbounded variants
- Latency benchmarks

**Tasks**:
1. Implement SPSC ring buffer with cache-padded atomics
2. Add send/recv methods with proper memory ordering
3. Implement try_send/try_recv variants
4. Write unit tests (single-threaded correctness)
5. Add multi-threaded tests (spawn threads, verify FIFO)
6. Implement MPMC with epoch-based ABA mitigation
7. Add backpressure handling (futex wait/wake)
8. Create ping-pong benchmark (target: < 1μs)
9. Property-based tests (no message loss, FIFO ordering)

**Success Criteria**:
- SPSC latency < 1μs for small messages
- MPMC throughput > 100K msgs/sec
- All tests pass (no data races)
- Property tests pass 10,000+ iterations

### Phase 4: Buffer Pool (Week 3-4)

**Deliverables**:
- BufferPool implementation with pre-allocation
- Buffer reference counting
- Zero-copy slice views
- Arrow RecordBatch integration

**Tasks**:
1. Implement BufferPool with contiguous backing store
2. Add acquire/release methods with free-list
3. Implement Buffer with Arc<BufferPool> sharing
4. Add zero-copy slice access (as_slice)
5. Integrate with channels (send Buffer, not bytes)
6. Add Arrow IPC serialization helpers
7. Write unit tests (acquire, release, no leaks)
8. Memory overhead benchmarks

**Success Criteria**:
- O(1) allocation/deallocation
- No memory leaks (valgrind validation)
- Overhead < 10% of data size
- Channel + buffer integration works

### Phase 5: Synchronization (Week 4-5)

**Deliverables**:
- SharedMutex with cross-process futex
- SharedRwLock implementation
- SharedCondvar implementation
- Fallback to file-based locking

**Tasks**:
1. Implement cross-process futex wrapper (no PRIVATE_FLAG)
2. Create SharedMutex with lock/unlock
3. Add try_lock and timeout variants
4. Implement SharedRwLock (read/write separation)
5. Implement SharedCondvar (wait/notify)
6. Add fallback detection and file-based locking
7. Write multi-process contention tests
8. Deadlock detection tests (timeouts)

**Success Criteria**:
- Cross-process mutual exclusion validated
- No deadlocks in stress tests
- Fallback works on systems without futex
- Contention benchmarks show acceptable overhead

### Phase 6: Binding Surfaces (Week 5-6)

**Deliverables**:
- PyO3 module exposing core IPC primitives
- napi-rs module exposing core IPC primitives
- Binding safety tests
- Memory leak validation

**Tasks**:
1. Implement PyO3 module exports for IPC primitives
2. Implement napi-rs exports for IPC primitives
3. Add error mapping (Rust → Python/JS errors)
4. Write binding safety tests (null/invalid input handling)
5. Add memory leak tests (valgrind)
6. Document binding usage patterns
7. Create Python + Bun integration examples

**Success Criteria**:
- IPC primitives callable from Python and Bun/Node
- No crashes with invalid inputs
- No memory leaks (valgrind clean)
- Examples execute successfully

### Phase 6b: Optional C ABI (If Required) (Week 6-7)

**Trigger**: Only if PyO3/napi-rs bindings are insufficient for native compatibility, stable ABI distribution, or additional language targets.

**Deliverables**:
- C ABI shim with opaque pointers
- Stable ABI surface documented and tested
- Language-neutral error mapping

**Tasks**:
1. Define minimal C ABI surface for core IPC primitives
2. Implement opaque pointer wrappers and explicit destroy APIs
3. Add C ABI tests and leak checks
4. Document C ABI usage and versioning strategy

### Phase 7: Integration and Documentation (Week 6-7)

**Deliverables**:
- End-to-end Python-Rust IPC example
- Performance benchmarking suite
- Comprehensive API documentation
- Migration guide from PRD-0

**Tasks**:
1. Create end-to-end example (Python producer/consumer)
2. Implement comprehensive benchmark suite
3. Generate rustdoc documentation
4. Write user guide (getting started, patterns)
5. Document performance tuning strategies
6. Run final Miri validation
7. Generate coverage reports (cargo-tarpaulin)
8. Prepare release artifacts

**Success Criteria**:
- Example demonstrates < 1μs latency
- Benchmarks show target performance met
- Documentation covers all public APIs
- Coverage ≥ 90% across all modules
- Miri passes on all code

### Timeline Summary

| Phase | Duration | Dependencies | Deliverable |
|-------|----------|--------------|-------------|
| 1. Foundation | Week 1 | None | Build system, CI |
| 2. Shared Memory | Week 1-2 | Phase 1 | ShmRegion with tests |
| 3. Channels | Week 2-3 | Phase 1 | SPSC/MPMC with benchmarks |
| 4. Buffer Pool | Week 3-4 | Phase 1 | BufferPool + Arrow |
| 5. Synchronization | Week 4-5 | Phase 1, 2 | Futex primitives |
| 6. Binding Surfaces | Week 5-6 | Phase 2-5 | PyO3 + napi-rs bindings |
| 6b. Optional C ABI | Week 6-7 | Phase 6 | C ABI shim (if required) |
| 7. Integration | Week 6-7 | All | Documentation, examples |

**Total Duration**: 7 weeks (35 business days)

**Milestones**:
- **Week 2**: Shared memory validated with Miri
- **Week 3**: SPSC channel achieves < 1μs latency
- **Week 5**: All core modules complete
- **Week 6**: Python + Bun/Node bindings functional
- **Week 7**: Final release ready

---

## Testing Strategy

### Unit Testing

**Scope**: Individual functions and modules in isolation.

**Coverage Goals**:
- shm.rs: 95% (critical for safety)
- channel.rs: 90%
- buffer.rs: 90%
- sync.rs: 90%
- Bindings: 85% (some code is error mapping/interop)

**Tools**:
- `cargo test` for standard tests
- `cargo-tarpaulin` for coverage reports
- `cargo-llvm-cov` for LLVM-based coverage

**Example**:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_create_and_open() {
        let name = "/test_shm_12345";
        let size = 4096;

        let region = ShmRegion::create(name, size).unwrap();
        assert_eq!(region.size(), size);

        let region2 = ShmRegion::open(name).unwrap();
        assert_eq!(region2.size(), size);
    }
}
```

### Integration Testing

**Scope**: Cross-module and cross-process interactions.

**Test Types**:
1. **Multi-process tests**: Spawn child processes, verify IPC
2. **Multi-threaded tests**: Spawn threads, verify concurrency
3. **FFI boundary tests**: Call C API from Rust tests
4. **Arrow IPC tests**: Serialize/deserialize RecordBatch

**Example**:
```rust
#[test]
fn test_cross_process_channel() {
    use std::process::Command;

    // Parent creates channel in shared memory
    let shm = ShmRegion::create("/test_channel", 65536).unwrap();
    let (tx, rx) = SpscChannel::new_in_shm(&mut shm, 1024);

    // Spawn child process
    let child = Command::new("/path/to/child_binary")
        .arg("/test_channel")
        .spawn()
        .unwrap();

    // Send message
    tx.send(b"Hello from parent").unwrap();

    // Wait for response
    let msg = rx.recv().unwrap();
    assert_eq!(msg, b"Hello from child");

    child.wait().unwrap();
}
```

### Property-Based Testing

**Scope**: Invariants that must hold for all inputs.

**Tool**: `proptest` crate

**Properties**:
1. **Channel FIFO**: Messages received in send order
2. **Channel lossless**: No messages dropped
3. **Buffer pool**: No double-free, no use-after-free
4. **Synchronization**: Mutual exclusion holds

**Example**:
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_channel_fifo_order(messages in prop::collection::vec(any::<u64>(), 1..1000)) {
        let (mut tx, mut rx) = SpscChannel::new(1024);

        // Send all messages
        for &msg in &messages {
            tx.send(msg).unwrap();
        }

        // Receive and verify order
        for &expected in &messages {
            let received = rx.recv().unwrap();
            assert_eq!(received, expected);
        }
    }
}
```

### Miri Validation

**Scope**: Undefined behavior detection in unsafe code.

**Command**: `cargo +nightly miri test`

**What Miri Detects**:
- Out-of-bounds accesses
- Use-after-free
- Data races (in unsafe code)
- Alignment violations
- Uninitialized memory reads

**Limitations**:
- Only tests one execution path per run
- Cannot validate FFI calls
- Slower execution (10-100x overhead)

**Strategy**:
- Run Miri on pure Rust code (vertebra-core)
- Use valgrind for binding layers (`vertebra-py`, `vertebra-js`)
- Multiple seeds for non-deterministic tests

### Benchmarking

**Tool**: Criterion.rs

**Benchmarks**:
1. **Ping-pong latency**: Round-trip message time
2. **Throughput**: Messages per second
3. **Memory allocation**: Buffer pool overhead
4. **Lock contention**: Mutex acquisition time

**Example**:
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_spsc_latency(c: &mut Criterion) {
    let (mut tx, mut rx) = SpscChannel::new(1024);

    c.bench_function("spsc_ping_pong", |b| {
        b.iter(|| {
            tx.send(black_box(42u64)).unwrap();
            let _ = rx.recv().unwrap();
        });
    });
}

criterion_group!(benches, bench_spsc_latency);
criterion_main!(benches);
```

**Success Criteria**:
- SPSC latency < 1μs (95th percentile)
- Throughput > 1M msgs/sec
- No performance regressions > 5%

---

## Risks and Mitigation

### High-Priority Risks

#### Risk 1: Cross-Process Futex Correctness

**Description**: Implementing cross-process futexes without `FUTEX_PRIVATE_FLAG` is complex and error-prone. Incorrect memory ordering or spurious wakeups could cause deadlocks or data corruption.

**Impact**: Critical (system unusable)

**Probability**: Medium (challenging but well-documented)

**Mitigation**:
1. Study existing implementations (Linux kernel, glibc)
2. Use model checking tools (SPIN, TLA+) to verify state machine
3. Extensive stress testing with multi-process contention
4. Implement fallback to file-based locking (flock)

**Contingency**:
- Start with file-based locking as baseline
- Add futex optimization as enhancement
- Document performance trade-offs

#### Risk 2: Memory Alignment for Arrow Data

**Description**: Apache Arrow requires 64-byte alignment for SIMD optimization. Shared memory allocations may not meet alignment requirements, causing crashes or performance degradation.

**Impact**: High (data corruption or poor performance)

**Probability**: Medium (depends on allocation patterns)

**Mitigation**:
1. Use `posix_memalign` or page-aligned shared memory
2. Static assertions validate alignment at compile time
3. Miri testing to detect alignment violations
4. Document alignment requirements in API

**Contingency**:
- Use copy-based approach if alignment issues persist
- Accept performance penalty for correctness
- Provide aligned allocator as optional feature

#### Risk 3: <1μs Latency Target

**Description**: Achieving sub-microsecond latency is challenging and depends on many factors (CPU, kernel, contention). Target may be infeasible on some hardware.

**Impact**: Medium (feature functional but slower)

**Probability**: Low-Medium (aggressive but achievable)

**Mitigation**:
1. Early benchmarking on target hardware
2. Profile hot paths with perf/flamegraph
3. Optimize critical sections (avoid syscalls, cache-friendly)
4. Document performance on reference hardware

**Contingency**:
- Relax requirement to < 5μs if < 1μs proves infeasible
- Provide "fast path" and "safe path" variants
- Document performance characteristics clearly

### Medium-Priority Risks

#### Risk 4: FFI Safety and Memory Leaks

**Description**: Manual memory management across FFI boundary is error-prone. Memory leaks or double-frees could occur if Python code doesn't call destroy functions.

**Impact**: Medium (memory leaks in long-running processes)

**Probability**: Medium (common FFI pitfall)

**Mitigation**:
1. Opaque pointers prevent direct memory access
2. Document ownership semantics clearly
3. Provide Python context managers (with __enter__/__exit__)
4. Valgrind tests for leak detection
5. Reference counting layer if manual management too complex

**Contingency**:
- Add automatic garbage collection via weak references
- Provide safe Rust API for embedding (not just FFI)

#### Risk 5: Miri Coverage Limitations

**Description**: Miri cannot validate FFI code or multi-process interactions. Some unsafe code may have UB that Miri doesn't detect.

**Impact**: Medium (undetected bugs in production)

**Probability**: Low (most code is pure Rust)

**Mitigation**:
1. Minimize unsafe code (isolate to small modules)
2. Document all unsafe assumptions
3. Use valgrind + AddressSanitizer for FFI
4. Formal code review for all unsafe blocks

**Contingency**:
- Manual code review process for unsafe code
- Conservative approach (prefer safe alternatives)

### Low-Priority Risks

#### Risk 6: Platform-Specific Issues

**Description**: Linux-specific APIs (futex, shm_open) may behave differently on different kernel versions or architectures.

**Impact**: Low (affects compatibility)

**Probability**: Low (POSIX APIs are stable)

**Mitigation**:
1. Test on multiple kernel versions (4.18, 5.x, 6.x)
2. Test on multiple architectures (x86_64, aarch64)
3. Runtime detection of kernel features
4. Fallback implementations where possible

**Contingency**:
- Document minimum kernel version requirements
- Provide feature detection at runtime

---

## Observability and Operations

### Logging Strategy

**Tool**: `tracing` crate (structured logging)

**Log Levels**:
- **ERROR**: Unrecoverable errors (crashes, data corruption)
- **WARN**: Recoverable errors (resource exhaustion, fallback activation)
- **INFO**: Lifecycle events (create, open, destroy)
- **DEBUG**: Operational details (send, recv, lock/unlock)
- **TRACE**: Hot path (disabled in production)

**Compilation**:
- Feature flag `logging` enables tracing
- Disabled by default in release builds (zero overhead)
- Enabled in debug builds for development

**FFI Logging**:
- Log entry/exit of all C API calls
- Include parameters (sanitized for security)
- Log error conditions before returning

### Metrics and Instrumentation

**Metrics to Track**:
1. **Channel metrics**:
   - Send/recv counts
   - Queue depth (current, max, avg)
   - Drop counts (messages lost due to full queue)

2. **Latency tracking**:
   - Histograms (P50, P90, P99, P99.9)
   - Per-operation latency (send, recv, lock, unlock)

3. **Memory metrics**:
   - Buffer pool utilization (%)
   - Shared memory region count
   - Total allocated bytes

4. **Error rates**:
   - Failed sends/recvs
   - Lock timeouts
   - Memory allocation failures

**Integration**:
- Python metrics exposed via Prometheus/StatsD
- Rust metrics via `metrics` crate
- Export to existing Vertebra monitoring

### Debugging Tools

**Development**:
- Debug builds with extra assertions (`#[cfg(debug_assertions)]`)
- Conditional compilation for expensive checks
- Memory debugging: valgrind, AddressSanitizer
- GDB/LLDB support: DWARF debug symbols

**Production**:
- Core dumps enabled for crash analysis
- Panic hooks log stack traces
- Shared memory inspection tools (custom CLI)

**Python Integration**:
- pdb integration for FFI debugging
- IPython introspection of Rust objects
- Stack traces across FFI boundary

### Performance Monitoring

**Profiling Tools**:
- **perf** + flamegraph for CPU profiling
- **DHAT** for memory allocation profiling
- **Criterion** for microbenchmarking
- **eBPF** for production profiling (minimal overhead)

**Key Metrics**:
- Latency distributions (target: < 1μs)
- Throughput (target: > 1M msgs/sec)
- CPU usage per operation
- Memory bandwidth utilization

---

## Documentation Deliverables

### Technical Documentation

1. **Architecture Document** (`docs/architecture/rust-ipc-core.md`)
   - System overview and design rationale
   - Component diagrams and interactions
   - Memory layout and data flow
   - Concurrency model and safety guarantees

2. **API Documentation** (via `cargo doc`)
   - Rustdoc for all public APIs
   - Safety invariants for unsafe code
   - Performance characteristics
   - Examples for common usage patterns

3. **Integration Guide** (`docs/guides/rust-python-integration.md`)
   - Building Rust from Python project
   - Makefile integration
   - Debugging Python-Rust interactions
   - Error handling patterns

4. **Performance Guide** (`docs/guides/performance-tuning.md`)
   - Benchmarking methodology
   - Profiling techniques
   - Optimization tips (buffer sizes, channel capacity)
   - Latency vs throughput trade-offs

### Developer Documentation

1. **Contributing Guide** (CONTRIBUTING.md update)
   - Rust code style (rustfmt, clippy)
   - Testing requirements
   - PR checklist for Rust code
   - Unsafe code review process

2. **Build Documentation** (README.md update)
   - Prerequisites (Rust toolchain, nightly for Miri)
   - Build commands
   - IDE setup (rust-analyzer, VSCode)
   - Troubleshooting

### User Documentation

1. **Getting Started Tutorial**
   - Hello World example (Python IPC)
   - Channel types and when to use each
   - Buffer pool configuration
   - Common patterns and anti-patterns

2. **API Reference**
   - Python API (generated from docstrings)
   - C API (header file documentation)
   - Error codes and handling

---

## Appendix

### Dependencies

#### External Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| rustix | 0.38 | POSIX abstractions (shm, futex, mmap) |
| arrow | 53.0 | Arrow IPC format |
| crossbeam-utils | 0.8 | Atomic utilities, cache padding |
| libc | 0.2 | Low-level C bindings |
| thiserror | 2.0 | Error handling |
| criterion | 0.5 | Benchmarking |
| proptest | 1.6 | Property-based testing |

#### System Requirements

| Component | Version | Notes |
|-----------|---------|-------|
| Linux kernel | 4.18+ | For modern futex APIs |
| Python | 3.12+ | Matching pyproject.toml and PRD-0 baseline |
| Rust | 1.75+ (stable) | For core development |
| Rust | nightly | For Miri testing |
| GCC/Clang | 9+ | For C compilation |

### Glossary

- **ABA Problem**: Concurrency issue where a value changes A→B→A, causing CAS to succeed incorrectly
- **Cache Padding**: Adding padding to prevent false sharing between cache lines
- **FFI**: Foreign Function Interface, calling non-Rust code from Rust
- **Futex**: Fast Userspace Mutex, Linux kernel primitive for synchronization
- **IPC**: Inter-Process Communication
- **Miri**: Rust interpreter for detecting undefined behavior
- **MPMC**: Multi-Producer Multi-Consumer
- **POSIX**: Portable Operating System Interface
- **RAII**: Resource Acquisition Is Initialization, automatic cleanup pattern
- **SPSC**: Single-Producer Single-Consumer
- **Zero-Copy**: Data sharing without duplication

### References

This PRD is informed by research documented in `docs/prd/PRD-1-rust-ipc/research/plan.md`. Core sources include:

1. **POSIX Shared Memory & Memory Mapping**
   - https://man7.org/linux/man-pages/man3/shm_open.3.html
   - https://man7.org/linux/man-pages/man2/mmap.2.html
2. **memfd for Anonymous Shared Memory**
   - https://man7.org/linux/man-pages/man2/memfd_create.2.html
3. **Rust POSIX Abstractions**
   - https://docs.rs/rustix/
4. **Async Runtime**
   - https://docs.rs/tokio/
5. **Miri**
   - https://github.com/rust-lang/miri
6. **PyO3 abi3**
   - https://pyo3.rs/main/building-and-distribution.html
7. **Node-API / napi-rs**
   - https://nodejs.org/api/n-api.html
   - https://napi.rs/
8. **Apache Arrow IPC**
   - https://arrow.apache.org/docs/

For detailed research notes, see `research/plan.md`.

---

## Progress Update

**Last reviewed:** 2026-01-17

### Completed (Foundation from PRD-0)
- Rust workspace created in `src/rs` with `vertebra-core`, `vertebra-py`, `vertebra-js`.
- PyO3 and napi-rs binding stubs exist and build.
- Makefile targets for Rust build/test/lint/miri/bench are in place.
- OS-API shim decisions recorded (memfd/shm_open/CreateFileMapping, futex/ulock/WaitOnAddress).
- Core module scaffolding created (`shm`, `channel`, `buffer`, `sync`, `platform`).
- Initial shared-memory mapping + wait/wake implementations in place (Linux/macOS/Windows).
- SPSC ring operations wired with fixed-slot layout.
- Buffer pool implemented (acquire/release, zero-copy slices).
- SharedMutex/RwLock/Condvar implemented with wait/wake shims.

### Next Steps
- Add benchmarks for ring latency and throughput (criterion).
- Implement robust shared-memory lifecycle (generation + heartbeat) and crash recovery tests.
- Add macOS pthread shared-cond fallback (optional, non-perf path).

---

## Changelog

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-17 | Claude Code | Initial draft |

---

**Approvals**:

- [ ] Technical Lead
- [ ] Product Owner
- [ ] Engineering Manager

**Status**: In Progress - Approved to implement
