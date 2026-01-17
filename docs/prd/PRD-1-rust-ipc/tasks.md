# PRD-1: Implementation Task Checklist

**Project**: Rust Core IPC Layer
**Version**: 1.0
**Last Updated**: 2026-01-17

---

## Phase 1: Foundation and Build System (Week 1)

### Setup

- [ ] 1.1.1 - Create `src/rs/` directory structure
- [ ] 1.1.2 - Initialize Cargo workspace in `src/rs/Cargo.toml`
- [ ] 1.1.3 - Set workspace resolver to "2"
- [ ] 1.1.4 - Define `[workspace.dependencies]` section

### Crate Initialization

- [ ] 1.2.1 - Create `vertebra-core` crate with `cargo new --lib`
- [ ] 1.2.2 - Create `vertebra-ffi` crate with `cargo new --lib`
- [ ] 1.2.3 - Configure `vertebra-core` with `crate-type = ["rlib"]`
- [ ] 1.2.4 - Configure `vertebra-ffi` with `crate-type = ["cdylib"]`

### Dependencies

- [ ] 1.3.1 - Add rustix (v0.38) to workspace dependencies
- [ ] 1.3.2 - Add arrow (v53.0) to workspace dependencies
- [ ] 1.3.3 - Add crossbeam-utils (v0.8) to workspace dependencies
- [ ] 1.3.4 - Add thiserror (v2.0) to workspace dependencies
- [ ] 1.3.5 - Add criterion (v0.5) to dev-dependencies
- [ ] 1.3.6 - Add proptest (v1.6) to dev-dependencies

### Project Structure

- [ ] 1.4.1 - Create `vertebra-core/src/lib.rs` with module declarations
- [ ] 1.4.2 - Create `vertebra-core/src/shm.rs` (stub)
- [ ] 1.4.3 - Create `vertebra-core/src/channel.rs` (stub)
- [ ] 1.4.4 - Create `vertebra-core/src/buffer.rs` (stub)
- [ ] 1.4.5 - Create `vertebra-core/src/sync.rs` (stub)
- [ ] 1.4.6 - Create `vertebra-ffi/src/lib.rs` with FFI exports

### Build Integration

- [ ] 1.5.1 - Add `rust-build` target to Makefile
- [ ] 1.5.2 - Add `rust-test` target to Makefile
- [ ] 1.5.3 - Add `rust-miri` target to Makefile
- [ ] 1.5.4 - Add `rust-bench` target to Makefile
- [ ] 1.5.5 - Add `rust-clean` target to Makefile

### CI/CD

- [ ] 1.6.1 - Add Rust toolchain installation to CI
- [ ] 1.6.2 - Add `cargo build` step to CI pipeline
- [ ] 1.6.3 - Add `cargo test` step to CI pipeline
- [ ] 1.6.4 - Add `cargo clippy` linting to CI
- [ ] 1.6.5 - Add `cargo fmt --check` to CI
- [ ] 1.6.6 - Add Miri tests to CI (nightly toolchain)

### Documentation

- [ ] 1.7.1 - Update README.md with Rust build instructions
- [ ] 1.7.2 - Create `docs/architecture/rust-ipc-core.md` (skeleton)
- [ ] 1.7.3 - Configure rustdoc in `Cargo.toml`
- [ ] 1.7.4 - Add doc comments to lib.rs
- [ ] 1.7.5 - Test `cargo doc` generation

---

## Phase 2: Shared Memory (Week 1-2)

### Implementation

- [ ] 2.1.1 - Define `ShmRegion` struct with fd, ptr, size fields
- [ ] 2.1.2 - Implement `ShmRegion::create()` using `rustix::shm::open`
- [ ] 2.1.3 - Call `rustix::fs::ftruncate()` to set size
- [ ] 2.1.4 - Call `rustix::mm::mmap()` with MAP_SHARED
- [ ] 2.1.5 - Close fd after mmap (not needed after mapping)
- [ ] 2.1.6 - Implement `ShmRegion::open()` for existing regions
- [ ] 2.1.7 - Implement `as_slice()` and `as_mut_slice()` methods
- [ ] 2.1.8 - Implement Drop trait with `munmap()` and `shm_unlink()`
- [ ] 2.1.9 - Add `ShmError` enum with thiserror derives
- [ ] 2.1.10 - Add `size()` method returning region size

### Safety

- [ ] 2.2.1 - Add SAFETY comments to all unsafe blocks
- [ ] 2.2.2 - Ensure exclusive mut access via borrow checker
- [ ] 2.2.3 - Validate name format (starts with `/`, max 255 chars)
- [ ] 2.2.4 - Ensure size is page-aligned (or align automatically)
- [ ] 2.2.5 - Set permissions to 0600 (owner only)

### Testing

- [ ] 2.3.1 - Test `create()` succeeds with valid inputs
- [ ] 2.3.2 - Test `open()` succeeds for existing region
- [ ] 2.3.3 - Test writing and reading data
- [ ] 2.3.4 - Test Drop cleans up resources
- [ ] 2.3.5 - Test error handling (invalid name, permissions)
- [ ] 2.3.6 - Add proptest for fuzz testing names/sizes
- [ ] 2.3.7 - Run `cargo +nightly miri test`
- [ ] 2.3.8 - Verify coverage ≥ 90% with cargo-tarpaulin

### Integration Testing

- [ ] 2.4.1 - Create multi-process test harness
- [ ] 2.4.2 - Test parent creates, child opens same region
- [ ] 2.4.3 - Test data written by parent is read by child
- [ ] 2.4.4 - Test cleanup when both processes exit
- [ ] 2.4.5 - Test concurrent access (read from multiple processes)

---

## Phase 3: Lock-Free Channels (Week 2-3)

### SPSC Implementation

- [ ] 3.1.1 - Define `SpscRingBuffer<T>` struct
- [ ] 3.1.2 - Add `buffer: Vec<T>` field
- [ ] 3.1.3 - Add `head: CachePadded<AtomicUsize>` (consumer)
- [ ] 3.1.4 - Add `tail: CachePadded<AtomicUsize>` (producer)
- [ ] 3.1.5 - Implement `SpscChannel::new(capacity)` with power-of-2 capacity
- [ ] 3.1.6 - Return `(Sender<T>, Receiver<T>)` pair
- [ ] 3.1.7 - Implement `Sender::send()` with Release ordering
- [ ] 3.1.8 - Implement `Receiver::recv()` with Acquire ordering
- [ ] 3.1.9 - Implement `try_send()` and `try_recv()` variants
- [ ] 3.1.10 - Handle full/empty conditions gracefully

### MPMC Implementation

- [ ] 3.2.1 - Define `MpmcRingBuffer<T>` struct
- [ ] 3.2.2 - Use `AtomicU64` for epoch-based ABA mitigation
- [ ] 3.2.3 - Implement `MpmcChannel::bounded(capacity)`
- [ ] 3.2.4 - Implement send with SeqCst ordering
- [ ] 3.2.5 - Implement recv with SeqCst ordering
- [ ] 3.2.6 - Add epoch increment on each operation
- [ ] 3.2.7 - Validate epoch before CAS success
- [ ] 3.2.8 - Handle contention with backoff strategy

### Backpressure

- [ ] 3.3.1 - Implement blocking `send()` when channel full
- [ ] 3.3.2 - Use futex wait/wake for blocking
- [ ] 3.3.3 - Add timeout variants (`send_timeout()`, `recv_timeout()`)
- [ ] 3.3.4 - Document backpressure behavior in API docs

### Testing

- [ ] 3.4.1 - Test SPSC single-threaded (FIFO ordering)
- [ ] 3.4.2 - Test SPSC multi-threaded (spawn producer/consumer)
- [ ] 3.4.3 - Test MPMC with multiple producers and consumers
- [ ] 3.4.4 - Add proptest for FIFO invariant (send order = recv order)
- [ ] 3.4.5 - Add proptest for lossless invariant (no message drops)
- [ ] 3.4.6 - Test full/empty edge cases
- [ ] 3.4.7 - Test backpressure blocking behavior
- [ ] 3.4.8 - Run Miri on channel tests

### Benchmarking

- [ ] 3.5.1 - Create ping-pong benchmark (SPSC)
- [ ] 3.5.2 - Measure round-trip latency (target: < 1μs)
- [ ] 3.5.3 - Create throughput benchmark (msgs/sec)
- [ ] 3.5.4 - Measure MPMC contention overhead
- [ ] 3.5.5 - Profile with perf + flamegraph
- [ ] 3.5.6 - Optimize hot paths based on profiling

---

## Phase 4: Buffer Pool (Week 3-4)

### Implementation

- [ ] 4.1.1 - Define `BufferPool` struct with backing Vec<u8>
- [ ] 4.1.2 - Add `buffer_size` and `count` fields
- [ ] 4.1.3 - Add `free_list: Mutex<Vec<usize>>` for indices
- [ ] 4.1.4 - Implement `BufferPool::new(buffer_size, count)` -> Arc<Self>
- [ ] 4.1.5 - Pre-allocate `buffer_size * count` bytes
- [ ] 4.1.6 - Initialize free_list with 0..count indices
- [ ] 4.1.7 - Define `Buffer` struct with pool: Arc<BufferPool>
- [ ] 4.1.8 - Add `offset` and `size` fields to Buffer
- [ ] 4.1.9 - Implement `acquire()` -> Result<Buffer>
- [ ] 4.1.10 - Lock free_list, pop index, calculate offset
- [ ] 4.1.11 - Implement Drop for Buffer (push index back to free_list)
- [ ] 4.1.12 - Implement `as_slice()` and `as_mut_slice()` for zero-copy

### Integration with Channels

- [ ] 4.2.1 - Modify channel to send `Buffer` instead of raw bytes
- [ ] 4.2.2 - Test zero-copy: acquire, send, recv, access data
- [ ] 4.2.3 - Verify no data copies during send/recv
- [ ] 4.2.4 - Document zero-copy pattern in API docs

### Arrow Integration

- [ ] 4.3.1 - Add `write_arrow_ipc()` function
- [ ] 4.3.2 - Use `arrow::ipc::writer::StreamWriter`
- [ ] 4.3.3 - Serialize RecordBatch to buffer
- [ ] 4.3.4 - Add `read_arrow_ipc()` function
- [ ] 4.3.5 - Use `arrow::ipc::reader::StreamReader`
- [ ] 4.3.6 - Deserialize RecordBatch from buffer
- [ ] 4.3.7 - Test round-trip serialization
- [ ] 4.3.8 - Verify zero-copy reading with memory-mapped buffers

### Testing

- [ ] 4.4.1 - Test acquire() returns valid buffer
- [ ] 4.4.2 - Test Drop returns buffer to pool
- [ ] 4.4.3 - Test no memory leaks (valgrind)
- [ ] 4.4.4 - Test concurrent acquire from multiple threads
- [ ] 4.4.5 - Test exhaustion (all buffers acquired)
- [ ] 4.4.6 - Add proptest for no double-free
- [ ] 4.4.7 - Add proptest for no use-after-free

### Benchmarking

- [ ] 4.5.1 - Measure allocation time (target: O(1))
- [ ] 4.5.2 - Measure deallocation time
- [ ] 4.5.3 - Measure memory overhead (target: < 10%)
- [ ] 4.5.4 - Compare against std::alloc for same pattern

---

## Phase 5: Synchronization (Week 4-5)

### Futex Wrapper

- [ ] 5.1.1 - Define `SharedMutex<T>` struct
- [ ] 5.1.2 - Add `futex: AtomicU32` field (0 = unlocked, 1 = locked)
- [ ] 5.1.3 - Add `value: UnsafeCell<T>` field
- [ ] 5.1.4 - Implement `lock()` with futex wait (NO FUTEX_PRIVATE_FLAG)
- [ ] 5.1.5 - Use compare_exchange with Acquire/Release ordering
- [ ] 5.1.6 - Call `rustix::thread::futex()` for wait
- [ ] 5.1.7 - Implement `unlock()` with futex wake
- [ ] 5.1.8 - Implement `try_lock()` without blocking
- [ ] 5.1.9 - Implement `lock_timeout()` with timeout variant

### MutexGuard

- [ ] 5.2.1 - Define `MutexGuard<'a, T>` with mutex reference
- [ ] 5.2.2 - Implement `Deref` for guard to access value
- [ ] 5.2.3 - Implement `DerefMut` for mutable access
- [ ] 5.2.4 - Implement Drop to unlock mutex

### RwLock

- [ ] 5.3.1 - Define `SharedRwLock<T>` struct
- [ ] 5.3.2 - Use reader count + writer flag pattern
- [ ] 5.3.3 - Implement `read()` for shared read access
- [ ] 5.3.4 - Implement `write()` for exclusive write access
- [ ] 5.3.5 - Implement `try_read()` and `try_write()`
- [ ] 5.3.6 - Add read/write guards with Drop

### Condvar

- [ ] 5.4.1 - Define `SharedCondvar` struct
- [ ] 5.4.2 - Implement `wait(&Mutex)` to atomically unlock and wait
- [ ] 5.4.3 - Implement `notify_one()` to wake one waiter
- [ ] 5.4.4 - Implement `notify_all()` to wake all waiters
- [ ] 5.4.5 - Ensure spurious wakeup handling

### Fallback

- [ ] 5.5.1 - Detect futex availability at runtime
- [ ] 5.5.2 - Implement file-based lock fallback (flock)
- [ ] 5.5.3 - Log warning when using fallback
- [ ] 5.5.4 - Document fallback behavior and performance

### Testing

- [ ] 5.6.1 - Test mutual exclusion (no data races)
- [ ] 5.6.2 - Test multi-process locking
- [ ] 5.6.3 - Test deadlock detection (timeouts)
- [ ] 5.6.4 - Test condvar wait/notify
- [ ] 5.6.5 - Stress test with high contention
- [ ] 5.6.6 - Run ThreadSanitizer for race detection
- [ ] 5.6.7 - Run Miri on synchronization code

### Benchmarking

- [ ] 5.7.1 - Measure lock/unlock latency
- [ ] 5.7.2 - Measure contention overhead (multiple threads)
- [ ] 5.7.3 - Compare futex vs file-based fallback
- [ ] 5.7.4 - Measure condvar notification latency

---

## Phase 6: FFI Layer (Week 5-6)

### C API Design

- [ ] 6.1.1 - Define opaque types in header file
- [ ] 6.1.2 - Define function signatures (C89 compatible)
- [ ] 6.1.3 - Define error codes (enum or constants)
- [ ] 6.1.4 - Document ownership semantics in comments

### Shared Memory FFI

- [ ] 6.2.1 - Implement `vertebra_shm_create(name, size)`
- [ ] 6.2.2 - Implement `vertebra_shm_open(name)`
- [ ] 6.2.3 - Implement `vertebra_shm_destroy(region)`
- [ ] 6.2.4 - Implement `vertebra_shm_data(region)` -> pointer
- [ ] 6.2.5 - Implement `vertebra_shm_size(region)` -> size
- [ ] 6.2.6 - Add null pointer checks
- [ ] 6.2.7 - Wrap all code in `catch_unwind` to prevent panics

### Channel FFI

- [ ] 6.3.1 - Implement `vertebra_spsc_create(capacity)`
- [ ] 6.3.2 - Implement `vertebra_spsc_destroy(channel)`
- [ ] 6.3.3 - Implement `vertebra_spsc_send(channel, data, len)`
- [ ] 6.3.4 - Implement `vertebra_spsc_recv(channel, data, len)`
- [ ] 6.3.5 - Return error codes on failure
- [ ] 6.3.6 - Add MPMC variants

### Buffer Pool FFI

- [ ] 6.4.1 - Implement `vertebra_buffer_pool_create(size, count)`
- [ ] 6.4.2 - Implement `vertebra_buffer_pool_destroy(pool)`
- [ ] 6.4.3 - Implement `vertebra_buffer_acquire(pool)`
- [ ] 6.4.4 - Implement `vertebra_buffer_release(buffer)`
- [ ] 6.4.5 - Implement `vertebra_buffer_data(buffer)` -> pointer

### Error Handling

- [ ] 6.5.1 - Define error enum with descriptive variants
- [ ] 6.5.2 - Implement thread-local error storage
- [ ] 6.5.3 - Implement `vertebra_error_message(code)` -> string
- [ ] 6.5.4 - Implement `vertebra_last_error()` for Python
- [ ] 6.5.5 - Document error handling in API docs

### Python Bindings

- [ ] 6.6.1 - Create Python module with cffi
- [ ] 6.6.2 - Load shared library (.so/.dylib)
- [ ] 6.6.3 - Define ffi.cdef() with C signatures
- [ ] 6.6.4 - Wrap C API in Pythonic classes
- [ ] 6.6.5 - Add context managers (`__enter__`/`__exit__`)
- [ ] 6.6.6 - Add type hints to all Python functions
- [ ] 6.6.7 - Document Python API with docstrings

### Testing

- [ ] 6.7.1 - Test all FFI functions from Rust
- [ ] 6.7.2 - Test null pointer handling
- [ ] 6.7.3 - Test invalid input handling
- [ ] 6.7.4 - Test memory leak detection (valgrind)
- [ ] 6.7.5 - Test from Python (import and call)
- [ ] 6.7.6 - Test Python context managers
- [ ] 6.7.7 - Test error handling from Python

---

## Phase 7: Integration and Documentation (Week 6-7)

### End-to-End Example

- [ ] 7.1.1 - Create Python producer script
- [ ] 7.1.2 - Create Python consumer script
- [ ] 7.1.3 - Demonstrate shared memory creation
- [ ] 7.1.4 - Demonstrate channel communication
- [ ] 7.1.5 - Demonstrate buffer pool usage
- [ ] 7.1.6 - Add timing measurements (verify < 1μs)
- [ ] 7.1.7 - Document example in README

### Benchmarking Suite

- [ ] 7.2.1 - Create comprehensive benchmark suite
- [ ] 7.2.2 - Benchmark all core operations
- [ ] 7.2.3 - Measure latency distributions (P50/P90/P99/P99.9)
- [ ] 7.2.4 - Measure throughput (msgs/sec)
- [ ] 7.2.5 - Generate flamegraphs for hot paths
- [ ] 7.2.6 - Document benchmark results
- [ ] 7.2.7 - Add regression tests (criterion)

### Documentation

- [ ] 7.3.1 - Complete architecture document
- [ ] 7.3.2 - Generate rustdoc for all modules
- [ ] 7.3.3 - Write getting started guide
- [ ] 7.3.4 - Write Python integration guide
- [ ] 7.3.5 - Write performance tuning guide
- [ ] 7.3.6 - Document error handling patterns
- [ ] 7.3.7 - Create API reference (Rust + Python + C)
- [ ] 7.3.8 - Add diagrams (component, data flow, memory layout)

### Final Validation

- [ ] 7.4.1 - Run full test suite (`make rust-test`)
- [ ] 7.4.2 - Run Miri on all code (`make rust-miri`)
- [ ] 7.4.3 - Run coverage report (target: 90%+)
- [ ] 7.4.4 - Run benchmarks (verify targets met)
- [ ] 7.4.5 - Run valgrind (no leaks)
- [ ] 7.4.6 - Run clippy (no warnings)
- [ ] 7.4.7 - Run rustfmt (consistent formatting)
- [ ] 7.4.8 - Run Python tests (integration)

### Release Preparation

- [ ] 7.5.1 - Tag version v0.1.0
- [ ] 7.5.2 - Generate changelog
- [ ] 7.5.3 - Build release artifacts
- [ ] 7.5.4 - Publish rustdoc to gh-pages
- [ ] 7.5.5 - Update top-level README
- [ ] 7.5.6 - Announce release
- [ ] 7.5.7 - Archive PRD documents

---

## Success Criteria Checklist

### Performance

- [ ] SPSC ping-pong latency < 1μs (P95)
- [ ] SPSC throughput > 1M msgs/sec
- [ ] MPMC throughput > 100K msgs/sec
- [ ] Memory overhead < 10% of data size
- [ ] No busy-waiting in hot paths

### Safety

- [ ] All Miri tests pass (no undefined behavior)
- [ ] All valgrind tests pass (no memory leaks)
- [ ] ThreadSanitizer passes (no data races)
- [ ] No crashes with invalid FFI inputs

### Coverage

- [ ] Unit test coverage ≥ 90%
- [ ] Integration tests cover all modules
- [ ] Property-based tests for critical invariants
- [ ] FFI tests from both Rust and Python

### Documentation

- [ ] All public APIs have doc comments
- [ ] Getting started guide complete
- [ ] Integration guide complete
- [ ] Performance guide complete
- [ ] API reference (Rust + Python + C)

### Integration

- [ ] Python FFI works seamlessly
- [ ] Makefile integration complete
- [ ] CI pipeline validates all code
- [ ] Examples demonstrate all features

---

## Notes

- Tasks are organized by phase and can be parallelized within constraints
- Dependencies between phases should be respected (e.g., FFI depends on core)
- Each checkbox represents a concrete, verifiable deliverable
- Tests should be written alongside implementation (TDD encouraged)
- Documentation should be updated as code evolves

**Total Tasks**: 225+
**Estimated Effort**: 7 weeks (1 engineer)
**Risk Buffer**: +1 week for unexpected issues

---

**Last Updated**: 2026-01-17
**Status**: Ready for Implementation
