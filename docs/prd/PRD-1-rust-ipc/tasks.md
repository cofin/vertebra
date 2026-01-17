# PRD-1: Implementation Task Checklist

**Project**: Rust Core IPC Layer
**Version**: 1.0
**Last Updated**: 2026-01-17

---

## Phase 1: Foundation and Build System (Week 1)

### Setup

- [x] 1.1.1 - Create `src/rs/` directory structure
- [x] 1.1.2 - Initialize Cargo workspace in `src/rs/Cargo.toml`
- [x] 1.1.3 - Set workspace resolver to "2"
- [x] 1.1.4 - Define `[workspace.dependencies]` section

### Crate Initialization

- [x] 1.2.1 - Create `vertebra-core` crate with `cargo new --lib`
- [x] 1.2.2 - Ensure `vertebra-py` crate exists (PyO3 bindings)
- [x] 1.2.3 - Ensure `vertebra-js` crate exists (napi-rs bindings)
- [x] 1.2.4 - Configure `vertebra-py`/`vertebra-js` with `crate-type = ["cdylib"]`

### Dependencies

- [x] 1.3.1 - Add rustix (v0.38) to workspace dependencies
- [x] 1.3.2 - Add arrow (v53.0) to workspace dependencies
- [x] 1.3.3 - Add crossbeam-utils (v0.8) to workspace dependencies
- [x] 1.3.4 - Add thiserror (v2.0) to workspace dependencies
- [x] 1.3.5 - Add criterion (v0.5) to dev-dependencies
- [x] 1.3.6 - Add proptest (v1.6) to dev-dependencies

### Project Structure

- [x] 1.4.1 - Create `vertebra-core/src/lib.rs` baseline (version constant + tests)
- [x] 1.4.2 - Create `vertebra-core/src/shm.rs` (stub)
- [x] 1.4.3 - Create `vertebra-core/src/channel.rs` (stub)
- [x] 1.4.4 - Create `vertebra-core/src/buffer.rs` (stub)
- [x] 1.4.5 - Create `vertebra-core/src/sync.rs` (stub)
- [x] 1.4.6 - Create binding stubs in `vertebra-py/src/lib.rs` and `vertebra-js/src/lib.rs`
- [x] 1.4.7 - Add `vertebra-core/src/platform/` with Linux/macOS/Windows backends (stubs)

### Build Integration

- [x] 1.5.1 - Add `rs-build` target to Makefile
- [x] 1.5.2 - Add `rs-test` target to Makefile
- [x] 1.5.3 - Add `rs-miri` target to Makefile
- [x] 1.5.4 - Add `rs-bench` target to Makefile
- [x] 1.5.5 - Add `rs-clean` target to Makefile

### CI/CD

- [x] 1.6.1 - Add Rust toolchain installation to CI
- [x] 1.6.2 - Add `cargo build` step to CI pipeline
- [x] 1.6.3 - Add `cargo test` step to CI pipeline
- [x] 1.6.4 - Add `cargo clippy` linting to CI
- [x] 1.6.5 - Add `cargo fmt --check` to CI
- [x] 1.6.6 - Add Miri tests to CI (nightly toolchain)
- [x] 1.6.7 - Add CI matrix for Linux/macOS/Windows (smoke build/tests)

### Documentation

- [x] 1.7.1 - Update README.md with Rust build instructions
- [x] 1.7.2 - Create `docs/architecture/rust-ipc-core.md` (skeleton)
- [x] 1.7.3 - Configure rustdoc in `Cargo.toml`
- [x] 1.7.4 - Add doc comments to lib.rs
- [x] 1.7.5 - Test `cargo doc` generation

---

## Phase 2: Shared Memory (Week 1-2)

### Implementation

- [x] 2.1.1 - Define `ShmRegion` struct with fd/handle, ptr, size fields
- [x] 2.1.2 - Implement `ShmRegion::create()` using OS APIs (memfd/shm_open/CreateFileMapping)
- [x] 2.1.3 - Call `ftruncate()` to set size
- [x] 2.1.4 - Call `mmap()` with MAP_SHARED (Unix) / `MapViewOfFile` (Windows)
- [x] 2.1.5 - Close fd after mmap (not needed after mapping)
- [x] 2.1.6 - Implement `ShmRegion::open_named()` for existing named regions (Unix)
- [x] 2.1.7 - Implement `as_slice()` and `as_mut_slice()` methods
- [x] 2.1.8 - Implement Drop with `munmap()` and optional `shm_unlink()`
- [x] 2.1.9 - Add `ShmError` enum with thiserror derives
- [x] 2.1.10 - Add `size()` method returning region size
- [x] 2.1.11 - Implement macOS shared memory backend (POSIX shm)
- [x] 2.1.12 - Implement Windows shared memory backend (CreateFileMapping/MapViewOfFile)

### Safety

- [x] 2.2.1 - Add SAFETY comments to all unsafe blocks
- [x] 2.2.2 - Ensure exclusive mut access via borrow checker (unsafe API guarded)
- [x] 2.2.3 - Validate name format (starts with `/`, max 255 chars)
- [x] 2.2.4 - Ensure size is page-aligned (or align automatically)
- [x] 2.2.5 - Set permissions to 0600 (owner only)

### Testing

- [x] 2.3.1 - Test `create()` succeeds with valid inputs
- [x] 2.3.2 - Test `open_named()` succeeds for existing region
- [x] 2.3.3 - Test writing and reading data
- [x] 2.3.4 - Test Drop cleans up resources
- [x] 2.3.5 - Test error handling (invalid name, permissions)
- [x] 2.3.6 - Add proptest for fuzz testing names/sizes
- [ ] 2.3.7 - Run `cargo +nightly miri test`
- [ ] 2.3.8 - Verify coverage ≥ 90% with cargo-tarpaulin

### Integration Testing

- [x] 2.4.1 - Create multi-process test harness
- [x] 2.4.2 - Test parent creates, child opens same region
- [x] 2.4.3 - Test data written by parent is read by child
- [x] 2.4.4 - Test cleanup when both processes exit
- [x] 2.4.5 - Test concurrent access (read from multiple processes)

---

## Phase 3: Lock-Free Channels (Week 2-3)

### SPSC Implementation

- [x] 3.1.1 - Define `SpscRing` struct (shared memory ring)
- [x] 3.1.2 - Use fixed-slot layout backed by shared memory
- [x] 3.1.3 - Add `head` (consumer) atomic in shared header
- [x] 3.1.4 - Add `tail` (producer) atomic in shared header
- [x] 3.1.5 - Implement `SpscRing::initialize/attach()` with power-of-2 capacity
- [x] 3.1.6 - Return `(Sender, Receiver)` pair abstraction (optional wrapper)
- [x] 3.1.7 - Implement `send()` with Release ordering
- [x] 3.1.8 - Implement `recv()` with Acquire ordering
- [x] 3.1.9 - Implement `try_send()` and `try_recv()` variants
- [x] 3.1.10 - Handle full/empty conditions gracefully

### MPMC Implementation

- [x] 3.2.1 - Define `MpmcRing` struct
- [x] 3.2.2 - Use `AtomicU64` for epoch-based ABA mitigation
- [x] 3.2.3 - Implement `MpmcChannel::bounded(capacity)`
- [x] 3.2.4 - Implement send with SeqCst ordering
- [x] 3.2.5 - Implement recv with SeqCst ordering
- [x] 3.2.6 - Add epoch increment on each operation
- [x] 3.2.7 - Validate epoch before CAS success
- [x] 3.2.8 - Handle contention with backoff strategy

### Backpressure

- [x] 3.3.1 - Implement blocking `send()` when channel full
- [x] 3.3.2 - Use wait/wake primitives for blocking
- [x] 3.3.3 - Add timeout variants (`send_timeout()`, `recv_timeout()`)
- [x] 3.3.4 - Document backpressure behavior in API docs

### Testing

- [x] 3.4.1 - Test SPSC single-threaded (FIFO ordering)
- [x] 3.4.2 - Test SPSC multi-threaded (spawn producer/consumer)
- [x] 3.4.3 - Test MPMC with multiple producers and consumers
- [x] 3.4.4 - Add proptest for FIFO invariant (send order = recv order)
- [x] 3.4.5 - Add proptest for lossless invariant (no message drops)
- [x] 3.4.6 - Test full/empty edge cases
- [x] 3.4.7 - Test backpressure blocking behavior
- [ ] 3.4.8 - Run Miri on channel tests

### Benchmarking

- [x] 3.5.1 - Create ping-pong benchmark (SPSC)
- [ ] 3.5.2 - Measure round-trip latency (target: < 1μs)
- [x] 3.5.3 - Create throughput benchmark (msgs/sec)
- [ ] 3.5.4 - Measure MPMC contention overhead
- [ ] 3.5.5 - Profile with perf + flamegraph
- [ ] 3.5.6 - Optimize hot paths based on profiling

---

## Phase 4: Buffer Pool (Week 3-4)

### Implementation

- [x] 4.1.1 - Define `BufferPool` struct with backing Vec<u8>
- [x] 4.1.2 - Add `buffer_size` and `count` fields
- [x] 4.1.3 - Add `free_list: Mutex<Vec<usize>>` for indices
- [x] 4.1.4 - Implement `BufferPool::new(buffer_size, count)` -> Arc<Self>
- [x] 4.1.5 - Pre-allocate `buffer_size * count` bytes
- [x] 4.1.6 - Initialize free_list with 0..count indices
- [x] 4.1.7 - Define `Buffer` struct with pool: Arc<BufferPool>
- [x] 4.1.8 - Add `index` + `buffer_size` accessors on Buffer
- [x] 4.1.9 - Implement `acquire()` -> Option<Buffer>
- [x] 4.1.10 - Lock free_list, pop index, calculate offset
- [x] 4.1.11 - Implement Drop for Buffer (push index back to free_list)
- [x] 4.1.12 - Implement `as_slice()` and `as_mut_slice()` for zero-copy

### Integration with Channels

- [x] 4.2.1 - Modify channel to send `Buffer` instead of raw bytes
- [x] 4.2.2 - Test zero-copy: acquire, send, recv, access data
- [x] 4.2.3 - Verify no data copies during send/recv
- [x] 4.2.4 - Document zero-copy pattern in API docs

### Arrow Integration

- [x] 4.3.1 - Add `write_arrow_ipc()` function
- [x] 4.3.2 - Use `arrow::ipc::writer::StreamWriter`
- [x] 4.3.3 - Serialize RecordBatch to buffer
- [x] 4.3.4 - Add `read_arrow_ipc()` function
- [x] 4.3.5 - Use `arrow::ipc::reader::StreamReader`
- [x] 4.3.6 - Deserialize RecordBatch from buffer
- [x] 4.3.7 - Test round-trip serialization
- [ ] 4.3.8 - Verify zero-copy reading with memory-mapped buffers

### Testing

- [x] 4.4.1 - Test acquire() returns valid buffer
- [x] 4.4.2 - Test Drop returns buffer to pool
- [ ] 4.4.3 - Test no memory leaks (valgrind)
- [x] 4.4.4 - Test concurrent acquire from multiple threads
- [x] 4.4.5 - Test exhaustion (all buffers acquired)
- [x] 4.4.6 - Add proptest for no double-free
- [x] 4.4.7 - Add proptest for no use-after-free

### Benchmarking

- [ ] 4.5.1 - Measure allocation time (target: O(1))
- [ ] 4.5.2 - Measure deallocation time
- [ ] 4.5.3 - Measure memory overhead (target: < 10%)
- [ ] 4.5.4 - Compare against std::alloc for same pattern

---

## Phase 5: Synchronization (Week 4-5)

### Futex Wrapper

- [x] 5.1.1 - Define `SharedMutex<T>` struct
- [x] 5.1.2 - Add `futex: AtomicU32` field (0 = unlocked, 1 = locked)
- [x] 5.1.3 - Add `value: UnsafeCell<T>` field
- [x] 5.1.4 - Implement `lock()` with wait/wake
- [x] 5.1.5 - Use compare_exchange with Acquire/Release ordering
- [x] 5.1.6 - Use platform wait/wake shim for blocking
- [x] 5.1.7 - Implement `unlock()` with wake
- [x] 5.1.8 - Implement `try_lock()` without blocking
- [x] 5.1.9 - Implement `lock_timeout()` with timeout variant

### MutexGuard

- [x] 5.2.1 - Define `MutexGuard<'a, T>` with mutex reference
- [x] 5.2.2 - Implement `Deref` for guard to access value
- [x] 5.2.3 - Implement `DerefMut` for mutable access
- [x] 5.2.4 - Implement Drop to unlock mutex

### RwLock

- [x] 5.3.1 - Define `SharedRwLock<T>` struct
- [x] 5.3.2 - Use reader count + writer flag pattern
- [x] 5.3.3 - Implement `read()` for shared read access
- [x] 5.3.4 - Implement `write()` for exclusive write access
- [x] 5.3.5 - Implement `try_read()` and `try_write()`
- [x] 5.3.6 - Add read/write guards with Drop

### Condvar

- [x] 5.4.1 - Define `SharedCondvar` struct
- [x] 5.4.2 - Implement `wait(&Mutex)` to atomically unlock and wait
- [x] 5.4.3 - Implement `notify_one()` to wake one waiter
- [x] 5.4.4 - Implement `notify_all()` to wake all waiters
- [x] 5.4.5 - Ensure spurious wakeup handling

### Fallback

- [x] 5.5.1 - Detect futex availability at runtime
- [x] 5.5.2 - Implement file-based lock fallback (flock)
- [x] 5.5.3 - Log warning when using fallback
- [x] 5.5.4 - Document fallback behavior and performance

### Testing

- [x] 5.6.1 - Test mutual exclusion (basic)
- [x] 5.6.2 - Test multi-process locking
- [ ] 5.6.3 - Test deadlock detection (timeouts)
- [x] 5.6.4 - Test condvar wait/notify
- [ ] 5.6.5 - Stress test with high contention
- [ ] 5.6.6 - Run ThreadSanitizer for race detection
- [ ] 5.6.7 - Run Miri on synchronization code

### Benchmarking

- [ ] 5.7.1 - Measure lock/unlock latency
- [ ] 5.7.2 - Measure contention overhead (multiple threads)
- [ ] 5.7.3 - Compare futex vs file-based fallback
- [ ] 5.7.4 - Measure condvar notification latency

---

## Phase 6: Binding Surfaces (Week 5-6)

### PyO3 Bindings

- [x] 6.1.1 - Expose shared memory APIs in `vertebra-py`
- [x] 6.1.2 - Expose channels and buffer pool APIs in `vertebra-py`
- [x] 6.1.3 - Map Rust errors to Python exceptions
- [x] 6.1.4 - Add PyO3 tests (lifecycle + invalid input)

### napi-rs Bindings

- [x] 6.2.1 - Expose shared memory APIs in `vertebra-js`
- [x] 6.2.2 - Expose channels and buffer pool APIs in `vertebra-js`
- [x] 6.2.3 - Map Rust errors to JS exceptions
- [ ] 6.2.4 - Add napi-rs tests (lifecycle + invalid input)

### Optional C ABI (If Required)

- [x] 6.3.1 - Decision gate: do we require C ABI for compatibility?
- [ ] 6.3.2 - If yes, define minimal C API surface
- [ ] 6.3.3 - Implement opaque pointer wrappers and explicit destroy APIs
- [ ] 6.3.4 - Add C ABI tests and leak checks
- [x] 6.6.5 - Add context managers (`__enter__`/`__exit__`)
- [ ] 6.6.6 - Add type hints to all Python functions
- [ ] 6.6.7 - Document Python API with docstrings

### Testing

- [ ] 6.7.1 - Test all FFI functions from Rust
- [ ] 6.7.2 - Test null pointer handling
- [ ] 6.7.3 - Test invalid input handling
- [ ] 6.7.4 - Test memory leak detection (valgrind)
- [x] 6.7.5 - Test from Python (import and call)
- [x] 6.7.6 - Test Python context managers
- [x] 6.7.7 - Test error handling from Python

---

## Phase 7: Integration and Documentation (Week 6-7)

### End-to-End Example

- [x] 7.1.1 - Create Python producer script
- [x] 7.1.2 - Create Python consumer script
- [x] 7.1.3 - Demonstrate shared memory creation
- [x] 7.1.4 - Demonstrate channel communication
- [x] 7.1.5 - Demonstrate buffer pool usage
- [x] 7.1.6 - Add timing measurements (verify < 1μs)
- [x] 7.1.7 - Document example in README

### Benchmarking Suite

- [ ] 7.2.1 - Create comprehensive benchmark suite
- [ ] 7.2.2 - Benchmark all core operations
- [ ] 7.2.3 - Measure latency distributions (P50/P90/P99/P99.9)
- [ ] 7.2.4 - Measure throughput (msgs/sec)
- [ ] 7.2.5 - Generate flamegraphs for hot paths
- [ ] 7.2.6 - Document benchmark results
- [ ] 7.2.7 - Add regression tests (criterion)

### Documentation

- [x] 7.3.1 - Complete architecture document
- [x] 7.3.2 - Generate rustdoc for all modules
- [x] 7.3.3 - Write getting started guide
- [x] 7.3.4 - Write Python integration guide
- [x] 7.3.5 - Write performance tuning guide
- [x] 7.3.6 - Document error handling patterns
- [x] 7.3.7 - Create API reference (Rust + Python + C)
- [x] 7.3.8 - Add diagrams (component, data flow, memory layout)

### Final Validation

- [x] 7.4.1 - Run full test suite (`make rust-test`)
- [ ] 7.4.2 - Run Miri on all code (`make rust-miri`)
- [ ] 7.4.3 - Run coverage report (target: 90%+)
- [ ] 7.4.4 - Run benchmarks (verify targets met)
- [ ] 7.4.5 - Run valgrind (no leaks)
- [ ] 7.4.6 - Run clippy (no warnings)
- [ ] 7.4.7 - Run rustfmt (consistent formatting)
- [x] 7.4.8 - Run Python tests (integration)

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
