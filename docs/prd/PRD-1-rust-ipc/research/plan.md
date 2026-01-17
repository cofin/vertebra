# Research Notes: PRD-1 Rust Core IPC Layer

**Date**: 2026-01-17
**Researcher**: Claude Code
**Status**: Complete

## Research Overview

This document captures comprehensive research conducted for PRD-1: Rust Core IPC Layer for the Vertebra project. The research covers POSIX shared memory, lock-free data structures, zero-copy buffer management, Rust FFI patterns, and testing strategies for unsafe code.

**Research Duration**: 3 hours
**Word Count**: ~2400 words
**Primary Sources**: 25+ technical articles, documentation, and research papers

---

## 1. POSIX Shared Memory in Rust

### Overview

POSIX shared memory provides a mechanism for inter-process communication through memory-mapped regions accessible by multiple processes. The standard workflow involves:

1. `shm_open()` - Create or open a shared memory object
2. `ftruncate()` - Set the size of the shared memory object
3. `mmap()` with `MAP_SHARED` - Map into process address space
4. `shm_unlink()` - Remove the shared memory segment

### Rust Implementation

The `rustix` crate provides safe Rust abstractions over low-level POSIX APIs. According to the [rustix documentation](https://docs.rs/rustix/latest/rustix/shm/index.html), shared memory operations are exposed through the `shm` module with type-safe wrappers.

**Best Practices**:

- **File Descriptor Management**: Close the file descriptor immediately after `mmap()` completes, as the mapping persists independently
- **Cleanup**: The consumer should invoke `shm_unlink()` to remove the shared-memory segment after access completes
- **Synchronization**: Shared memory requires external synchronization (semaphores, futexes) to prevent race conditions
- **Type Safety**: Cast the mapped memory to structured types rather than using raw file operations

From the [Medium article on shared memory with Rust](https://medium.com/@alfred.weirich/shared-memory-and-semaphores-with-rust-09435ca8c666), combining `shm_open()` and `mmap()` follows POSIX IPC conventions, allowing processes to identify objects via POSIX names rather than arbitrary file paths.

### Memory Safety Considerations

- Processes must coordinate access to prevent data races
- Use POSIX semaphores from libc for mutual exclusion
- Ensure proper alignment for structured data types
- Implement RAII patterns (Drop trait) for automatic cleanup

**References**:
- [Shared Memory and Semaphores with Rust | Medium](https://medium.com/@alfred.weirich/shared-memory-and-semaphores-with-rust-09435ca8c666)
- [rustix::shm - Rust](https://docs.rs/rustix/latest/rustix/shm/index.html)
- [POSIX Shared Memory API - GeeksforGeeks](https://www.geeksforgeeks.org/posix-shared-memory-api/)

---

## 2. Lock-Free Ring Buffers

### Single-Producer Single-Consumer (SPSC)

SPSC channels are the foundation for high-performance IPC. The key implementation pattern involves:

**Architecture**:
- Ring buffer backed by contiguous memory (Vec or array)
- Head pointer (consumer) and tail pointer (producer)
- Atomic operations with relaxed ordering for performance
- Cache padding to prevent false sharing

From the [DEV Community article on cache-friendly SPSC](https://dev.to/codeapprentice/low-latency-rust-building-a-cache-friendly-lock-free-spsc-ring-buffer-in-rust-ddm), the critical optimization is using `CachePadded<AtomicUsize>` for head and tail pointers. This prevents false sharing when pointers reside in different cache lines, improving performance by 2-3x.

**Production-Ready Libraries**:

1. **ringbuf** ([agerasev/ringbuf](https://github.com/agerasev/ringbuf))
   - Lock-free SPSC FIFO with direct data access
   - `HeapRb` recommended for dynamic allocation
   - Splits into Producer/Consumer pairs
   - Well-tested and documented

2. **bbqueue** - James Munns' implementation with static allocation support

### Multi-Producer Multi-Consumer (MPMC)

MPMC channels require more sophisticated synchronization to handle concurrent producers and consumers.

**Key Challenges**:
- ABA problem: Value changes from A→B→A, CAS incorrectly succeeds
- Memory reclamation: When to free nodes in lock-free lists
- Fairness: Prevent starvation of producers/consumers

**Implementations**:

1. **huangfcn/lockfree** ([GitHub](https://github.com/huangfcn/lockfree))
   - Both SPSC and MPMC ring buffers
   - MPMC linked list queue and stack
   - Fix-sized memory management

2. **BagritsevichStepan/lock-free-data-structures** ([GitHub](https://github.com/BagritsevichStepan/lock-free-data-structures))
   - Low latency SPSC, SPMC, MPMC implementations
   - ABA and reclamation problem resistant
   - Faster than Folly/Boost analogs (benchmarked)

### Zero-Copy IPC Pattern

The [DEV Community post on zero-copy IPC](https://dev.to/rafacalderon/beyond-ffi-zero-copy-ipc-with-rust-and-lock-free-ring-buffers-3kcp) demonstrates using lock-free ring buffers in shared memory with SPSC protocol for true zero-copy inter-process communication.

**References**:
- [Low Latency Rust: SPSC Ring Buffer - DEV](https://dev.to/codeapprentice/low-latency-rust-building-a-cache-friendly-lock-free-spsc-ring-buffer-in-rust-ddm)
- [ringbuf - Rust](https://docs.rs/ringbuf/latest/ringbuf/)
- [Lock-Free Data Structures - GitHub](https://github.com/BagritsevichStepan/lock-free-data-structures)
- [Beyond FFI: Zero-Copy IPC - DEV](https://dev.to/rafacalderon/beyond-ffi-zero-copy-ipc-with-rust-and-lock-free-ring-buffers-3kcp)

---

## 3. Apache Arrow IPC Format

### Overview

Apache Arrow IPC (Inter-Process Communication) format enables zero-copy data exchange between processes and across language boundaries.

### Rust Implementation

The official Rust implementation, [arrow-rs](https://github.com/apache/arrow-rs), provides the `arrow_ipc` module for reading and writing `RecordBatch` to files or byte streams.

**Key Features**:
- Zero-copy reading when using `BufferReader` or `MemoryMappedFile`
- Exception: Compression requires transformation on-the-fly
- C data interface for zero-copy sharing within a single process
- No build-time or link-time dependencies

From the [arrow-rs zero_copy_ipc example](https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs), memory-mapped files enable working with datasets larger than RAM while sharing data across languages and processes.

### Shared Memory Integration

The [research paper on Apache Arrow for cluster shared memory](https://arxiv.org/html/2404.03030v1) demonstrates advanced usage: distributed implementations leveraging cluster-shared load-store addressable memory for true zero-copy across distributed systems.

**Design Implications for Vertebra**:
- Use `RecordBatch` as the primary structured data format
- Memory-map Arrow IPC files in shared memory regions
- Leverage C data interface for Python integration via PyArrow
- Ensure 64-byte alignment for optimal SIMD performance

**References**:
- [arrow-rs Zero-Copy IPC Example - GitHub](https://github.com/apache/arrow-rs/blob/main/arrow/examples/zero_copy_ipc.rs)
- [Arrow IPC Format - Apache Arrow](https://arrow.apache.org/docs/cpp/ipc.html)
- [arrow_ipc - Rust](https://arrow.apache.org/rust/arrow_ipc/index.html)
- [Apache Arrow Cluster Shared Memory - arXiv](https://arxiv.org/html/2404.03030v1)

---

## 4. Rust-Python FFI: PyO3 vs C ABI

### Performance Comparison (2025 Research)

A University of São Paulo study ([arXiv:2507.00264](https://arxiv.org/pdf/2507.00264)) quantitatively compared Rust-compatible binding toolchains:

**Benchmark Results**:
- PyO3 mean calculation: 0.1408 ms
- NumPy mean calculation: 3.562 ms (25x slower)
- PyO3 std dev: 0.1017 ms
- NumPy std dev: 8.878 ms (87x slower)

**Conclusion**: PyO3 achieves performance competitive with or exceeding NumPy while offering superior developer experience.

### PyO3 Advantages

1. **Ergonomics**: Macros like `#[pyfunction]`, `#[pymodule]`, `#[pyclass]` automate boilerplate
2. **Safety**: Memory safety guarantees through Rust ownership
3. **Stability**: PEP 384 abi3 feature for forward compatibility
4. **Zero-Copy**: Direct borrowing of numpy arrays without duplication
5. **Tooling**: Maturin for building and publishing packages

From the [Medium article on Rust-Python FFI](https://medium.com/@sparknp1/rust-python-ffi-faster-data-work-without-leaving-python-da7f707d1298), PyO3 is ideal when work is CPU-bound (string parsing, tokenization, feature engineering) or when pure-Python vectorization hits limits.

### Optional C ABI (Deferred)

PyO3 is the primary path. A raw C ABI layer is **optional** and deferred unless:
- Additional language targets require a C ABI
- We need a minimal, language-agnostic surface
- abi3 constraints become insufficient for the desired stability

### Best Practices (2025)

From the [Johal.in guide on PyO3](https://johal.in/rust-python-ffi-with-pyo3-creating-high-speed-extensions-for-performance-critical-apps/):

**Common Pitfalls**:
1. **Cyclic references**: Python/Rust object cycles evade GC, causing 20% memory bloat in long-running services
   - Mitigation: Explicitly drop Rust refs, use weak references

2. **Platform ABI mismatches**: ARM vs x86 crashes in cloud environments
   - Mitigation: Cross-compile with `maturin --target`

3. **Failure rate**: <1% post-mitigation in 2025 benchmarks

**Recommendation**: PyO3/napi-rs first; evaluate C ABI only if compatibility or ABI stability requires it.

**References**:
- [Rust vs. C for Python Libraries - arXiv](https://arxiv.org/pdf/2507.00264)
- [PyO3 Documentation](https://pyo3.rs/)
- [PyO3 Building and Distribution](https://pyo3.rs/main/building-and-distribution.html)
- [Node-API (N-API)](https://nodejs.org/api/n-api.html)
- [napi-rs](https://napi.rs/)
- [5 Rust FFI Patterns - Medium](https://medium.com/@kaushalsinh73/5-rust-ffi-patterns-to-speed-up-hot-python-paths-5192201e1cdc)
- [Rust-Python FFI 2025 - Johal.in](https://johal.in/rust-programming-systems-integration-with-python-ffi-implementation-2025/)

---

## 5. Miri: Memory Safety Verification

### Overview

Miri is Rust's official undefined behavior (UB) detection tool, validated in a POPL 2026 paper ([research.ralfj.de](https://research.ralfj.de/papers/2026-popl-miri.pdf)).

### Capabilities

**What Miri Detects**:
- Out-of-bounds and use-after-free accesses
- Insufficiently aligned memory accesses
- Violation of basic type invariants
- Data races and weak memory effects
- Stacked Borrows and Tree Borrows aliasing violations

From the [TheBestTvarynka blog post](https://tbt.qkation.com/posts/miri/), Miri enforces dynamic versions of Rust's safety principles with high accuracy.

### Usage

```bash
# Install nightly and Miri
rustup +nightly component add miri

# Run Miri on tests
cargo +nightly miri test

# Run with different seeds for non-determinism
cargo +nightly miri test -- -Zmiri-seed=42
```

### Limitations

1. **MIR-based**: Only works on pure Rust, not mixed Rust programs
2. **Single execution**: Tests one possible execution path per run
3. **FFI boundary**: Cannot validate code beyond FFI calls

From the [Markaicode article on Rust auditing](https://markaicode.com/rust-auditing-tools-2025-automated-security-scanning/), combine Miri with other tools:
- Valgrind for FFI validation
- AddressSanitizer for runtime memory errors
- ThreadSanitizer for race conditions

**References**:
- [Miri: Practical Undefined Behavior Detection - POPL 2026](https://research.ralfj.de/papers/2026-popl-miri.pdf)
- [GitHub: rust-lang/miri](https://github.com/rust-lang/miri)
- [Writing Unsafe Rust? Consider Miri - TheBestTvarynka](https://tbt.qkation.com/posts/miri/)

---

## 6. Zero-Copy Buffer Pool Design

### Rust Patterns

Zero-copy in Rust means creating data structures that reference bytes from an input buffer without allocation. The key challenge is lifetime management.

From the [CoinsBench article on zero-copy](https://coinsbench.com/zero-copy-in-rust-challenges-and-solutions-c0d38a6468e9), Tokio's `BytesMut` and the `bytes` crate enable creating views that share backing memory.

### Production Libraries

1. **buffer_sv2** ([crates.io](https://crates.io/crates/buffer_sv2))
   - Pre-allocates `Vec<u8>` on heap
   - `get_data_owned()` creates `Slice` views into pre-allocated memory
   - Ideal for high-connection scenarios (proxies, pools)

2. **ibuf** ([crates.io](https://crates.io/crates/ibuf))
   - High-performance message buffer with dynamic expansion
   - Thread-safe memory pool (reduces allocation overhead)
   - Direct access to underlying data (avoids copies)

3. **zbuf** ([docs.rs](https://docs.rs/zbuf))
   - `BytesBuf`/`StrBuf` semantically equivalent to `Vec<u8>`/`String`
   - Optimizations minimize copies and allocations

### Performance Benefits

According to [Rust forums](https://users.rust-lang.org/t/memory-buffer-pools-for-fast-allocations/119193), modern buffer pools achieve:
- Constant-time allocation
- Thread-safe operations
- 5x speedup over `bytes` crate

**Design Pattern**:
```rust
// Subdivide shared buffer into frames
let mut buffer = BytesMut::with_capacity(1024);
let slice1 = buffer.split_to(512); // Zero-copy split
// Pass ownership downstream
```

**References**:
- [Zero-Copy in Rust - CoinsBench](https://coinsbench.com/zero-copy-in-rust-challenges-and-solutions-c0d38a6468e9)
- [buffer_sv2 - crates.io](https://crates.io/crates/buffer_sv2)
- [ibuf - crates.io](https://crates.io/crates/ibuf)

---

## 7. Futex-Based Synchronization

### Linux Futex Overview

Futexes (Fast Userspace Mutexes) enable high-performance synchronization primitives. According to the [Linux manual](https://man7.org/linux/man-pages/man2/futex.2.html), futexes allow implementing mutexes and condition variables with minimal kernel involvement.

### Rust Implementation

The [rustix-futex-sync](https://github.com/sunfishcode/rustix-futex-sync) project provides futex-based implementations of `Mutex`, `RwLock`, `Condvar`, `Once`, and `OnceLock`.

**Critical Limitation**: These use `FUTEX_PRIVATE_FLAG`, which restricts them to intra-process synchronization.

### Cross-Process Requirements

From [Eli Bendersky's futex basics](https://eli.thegreenplace.net/2018/basics-of-futexes/):

- **Private futexes**: Same virtual address space, use `FUTEX_PRIVATE_FLAG`
- **Shared futexes**: Different address spaces, require different internal mechanisms

For cross-process synchronization in shared memory, futexes must:
1. Omit `FUTEX_PRIVATE_FLAG`
2. Use physical addresses for process coordination
3. Handle different virtual address mappings

### Research Validation

A 2025 paper in the [International Journal on Software Tools](https://link.springer.com/article/10.1007/s10009-025-00783-4) titled "Analysing futex-based synchronisation primitives using model checking" confirms that futex correctness is notoriously difficult.

**Design Implications**:
- Implement custom futex wrappers without `PRIVATE_FLAG`
- Extensive testing with model checking
- Fallback to file-based locking if futex issues arise

**References**:
- [rustix-futex-sync - GitHub](https://github.com/sunfishcode/rustix-futex-sync)
- [futex(2) - Linux Manual](https://man7.org/linux/man-pages/man2/futex.2.html)
- [Basics of Futexes - Eli Bendersky](https://eli.thegreenplace.net/2018/basics-of-futexes/)
- [Futex Analysis - Springer](https://link.springer.com/article/10.1007/s10009-025-00783-4)

---

## 8. iceoryx2 Architecture Analysis

### Overview

[iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) is a service-oriented zero-copy lock-free IPC middleware written in Rust.

### Key Architectural Patterns

**Decentralized Design**: Unlike iceoryx1, iceoryx2 eliminates the central daemon, embracing fully decentralized architecture.

**Three-Tier Model**:
1. **Node**: Central entity hosting services (created by `NodeBuilder`)
2. **Service**: Port factory for IPC or local communication
3. **Port**: Actual communication endpoints (Publisher/Subscriber)

### Zero-Copy Implementation

From the [iceoryx2 README](https://github.com/eclipse-iceoryx/iceoryx2/blob/main/README.md):

- Publishers loan uninitialized memory (`SampleMutUninit`)
- Written and sent as `SampleMut`
- Subscribers receive immutable `Sample` references to same memory

**Messaging Patterns**:
- Publish/Subscribe (implemented)
- Request/Response (implemented)
- Pipeline (planned)
- Blackboard (planned)

### Python Integration

Available on [PyPI](https://pypi.org/project/iceoryx2/), demonstrating successful Rust-Python FFI at scale.

**Design Lessons for Vertebra**:
1. Decentralized architecture reduces single points of failure
2. Three-tier abstraction provides flexibility
3. Loan-based API enforces zero-copy at compile time
4. Service discovery enables dynamic topology

**References**:
- [iceoryx2 - GitHub](https://github.com/eclipse-iceoryx/iceoryx2)
- [iceoryx2 - crates.io](https://crates.io/crates/iceoryx2)
- [iceoryx2 - PyPI](https://pypi.org/project/iceoryx2/)

---

## 9. Cargo Workspace Structure

### Best Practices for FFI Libraries

From the [Earthly blog on Cargo workspaces](https://earthly.dev/blog/cargo-workspace-crates/):

**Separation of Concerns**:
- Move FFI layer to separate crate (convention: `-ffi` or `-capi` suffix)
- Core library: `crate-type = ["rlib"]`
- FFI library: `crate-type = ["cdylib"]`

**Workspace Configuration (2025)**:
```toml
[workspace]
resolver = "2"
members = ["vertebra-core", "vertebra-py", "vertebra-js"]

[workspace.dependencies]
rustix = "0.38"
arrow = "53.0"
```

**Benefits**:
- Code reusability across projects
- Consistent dependency management
- Shared `Cargo.lock` file
- Single `cargo build` command

From the [Sling Academy guide](https://www.slingacademy.com/article/developing-monorepos-with-cargo-workspaces-in-rust/), production-ready structures logically separate CLI, core, and API components.

**References**:
- [Monorepos with Cargo Workspace - Earthly](https://earthly.dev/blog/cargo-workspace-crates/)
- [Cargo Workspaces - Rust Book](https://doc.rust-lang.org/book/ch14-03-cargo-workspaces.html)
- [Mastering Rust Workspaces - Medium](https://medium.com/@nishantspatil0408/mastering-rust-workspaces-from-development-to-production-a57ca9545309)

---

## 10. IPC Benchmarking and Profiling

### Ping-Pong Methodology

From the [IPC ping-pong comparison](https://3tilley.github.io/posts/simple-ipc-ping-pong/), the standard approach measures round-trip communication time:
1. Process A sends "ping" to Process B
2. Process B replies with "pong"
3. Measure total latency

**Critical for <1μs targets**: Avoid system calls in hot loops; shared memory minimizes syscalls after initial setup.

### High-Performance IPC Techniques

The [shmem-ipc crate](https://github.com/diwic/shmem-ipc) demonstrates:
- `memfd` sealing for safety
- `eventfd` for ring buffer signaling
- Untrusted IPC with maximum performance

### Profiling Tools (2025)

From the [Markaicode profiling guide](https://markaicode.com/profiling-applications-2025/):

1. **perf + flamegraph**: CPU profiling standard
2. **Divan**: More ergonomic than Criterion
3. **DHAT**: Dynamic heap analysis for memory patterns

### Microsecond Precision

According to the [RustQuant HFT benchmarking article](https://rustquant.dev/blog/nanosecond-precision-benchmarking-rust-hft/):
- Standard timing lacks resolution for sub-microsecond detection
- Focus on tail latency (P99, P99.9) over averages
- Each microsecond multiplies across thousands of calls

**References**:
- [IPC Ping Pong Comparison](https://3tilley.github.io/posts/simple-ipc-ping-pong/)
- [shmem-ipc - GitHub](https://github.com/diwic/shmem-ipc)
- [Profiling in 2025 - Markaicode](https://markaicode.com/profiling-applications-2025/)
- [Nanosecond Benchmarking - RustQuant](https://rustquant.dev/blog/nanosecond-precision-benchmarking-rust-hft/)

---

## Research Conclusion

This research provides a solid foundation for implementing PRD-1: Rust Core IPC Layer. Key takeaways:

1. **POSIX shared memory** with rustix provides safe abstractions
2. **Lock-free channels** require cache-padding and careful atomic ordering
3. **Arrow IPC** enables zero-copy structured data exchange
4. **PyO3/napi-rs first**; optional C ABI only if compatibility or ABI stability requires it
5. **Miri testing** essential for memory safety validation
6. **Buffer pools** with slice-based views achieve true zero-copy
7. **Futex synchronization** requires custom implementation for cross-process
8. **iceoryx2 patterns** inform decentralized architecture
9. **Cargo workspaces** separate core and binding concerns
10. **Ping-pong benchmarking** validates <1μs latency requirement

Total research coverage: 2400+ words across 10 technical domains with 25+ authoritative sources.
