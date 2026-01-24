# Recovery Guide: Rust Fast Path for Sub-10us Python IPC

## Quick Resume

- **Last Phase**: Planning Complete
- **Current Status**: PRD and tasks created, ready for implementation
- **PRD Location**: `specs/active/rust-fast-path/`

## Intelligence Context

### Complexity
High - Multi-language optimization across Python and Rust with careful lifetime management

### Stack
- **Languages**: Python 3.12+, Rust
- **Build**: Make-based with Cargo for Rust

### Commands

| Action | Command |
|--------|---------|
| Install | `make install` |
| Test (Python) | `make test` |
| Test (Rust) | `make rs-test` |
| Test (All) | `make test-all` |
| Lint | `make lint-all` |
| Single Rust test | `cd src/rs && cargo test --package vertebra-core test_name` |
| Benchmark | `uv run python benchmarks/ipc_fast_path.py` |

### Documentation
- System: Markdown in `docs/`
- IPC Guide: `docs/architecture/python-ipc-guide.md`
- Performance: `docs/architecture/performance-tuning.md`

### Similar Implementations Referenced

1. `src/rs/vertebra-core/src/notify.rs` - InheritableNotifier (extend with spin_wait_or_park)
2. `src/rs/vertebra-py/src/lib.rs:258-613` - PyBuffer with buffer protocol (pattern for PySlotView)
3. `src/py/vertebra/ipc/channel.py:87-165` - BufferView returning memoryview
4. `src/rs/vertebra-py/src/supervisor.rs` - FD inheritance pattern

### Patterns Being Followed

- **Hybrid Spin-Wait**: Spin for configurable us before blocking on eventfd
- **Buffer Protocol**: Use __getbuffer__/__releasebuffer__ for zero-copy
- **FD Inheritance**: Pass eventfd via VERTEBRA_NOTIFY_FD environment variable
- **Event Loop Integration**: Use asyncio.add_reader() for non-blocking async

## State Summary

### Completed

- [x] Problem analysis (GIL, allocation, to_thread overhead identified)
- [x] Multi-model consensus (Gemini 3 Pro FOR and AGAINST)
- [x] Three-tier targets defined (sync <10us, async <20us)
- [x] Research document (2500+ words)
- [x] PRD document (3400+ words)
- [x] Task breakdown (18 tasks, 4 phases)
- [x] Benchmark suite created (`benchmarks/ipc_fast_path.py`)

### In Progress

- [ ] Phase 1: Hybrid Spin-Wait (Foundation)
  - [ ] Task 1.1: Add spin_wait_or_park to InheritableNotifier
  - [ ] Task 1.2: Add recv_hybrid to SpscRing
  - [ ] Task 1.3: Expose hybrid recv in PyChannel
  - [ ] Task 1.4: Add VERTEBRA_SPIN_US configuration
  - [ ] Task 1.5: Benchmark validation

- [ ] Phase 2: Zero-Copy SlotView
  - [ ] Task 2.1: Add SlotView struct to channel.rs
  - [ ] Task 2.2: Add recv_to_slot() returning SlotView
  - [ ] Task 2.3: Expose PySlotView with buffer protocol
  - [ ] Task 2.4: Update Python Channel to use slot views

- [ ] Phase 3: asyncio.add_reader Integration
  - [ ] Task 3.1: Add notify_fd property to PyChannel
  - [ ] Task 3.2: Add async recv using add_reader
  - [ ] Task 3.3: Update AsyncBrokerClient
  - [ ] Task 3.4: Validate with uvloop

- [ ] Phase 4: Rust-Native Correlation Tracking
  - [ ] Task 4.1: Add protocol parsing to Rust
  - [ ] Task 4.2: Add PendingMap to PyFastChannel
  - [ ] Task 4.3: Add send_request_fast()
  - [ ] Task 4.4: Add poll_response_fast()
  - [ ] Task 4.5: Add request_response_fast() combining both

### Pending

- [x] Benchmark suite creation (done)
- [ ] Documentation updates
- [ ] Performance validation (after each phase)

## Key Design Decisions

### 1. Hybrid Spin-Wait (NOT Pure Blocking)

**Decision**: Spin for 2-5us before blocking on eventfd.

**Rationale**:
- Pure eventfd wake has 3-5us latency
- Spinning captures immediate responses without syscall overhead
- Configurable via VERTEBRA_SPIN_US environment variable
- Multi-model consensus mandated this approach

### 2. SlotView with Guard (NOT Buffer Index)

**Decision**: Return memoryview backed by SlotView with lifetime guard.

**Rationale**:
- Buffer index API degrades developer experience
- SlotView holds Arc<SlotGuard> blocking ring advance
- Automatic release when Python releases memoryview
- Follows existing BufferView pattern in codebase

### 3. asyncio.add_reader (NOT to_thread)

**Decision**: Use native FD polling for async instead of thread pool.

**Rationale**:
- asyncio.to_thread adds 20us overhead
- add_reader uses kernel-level notification (epoll/kqueue)
- Standard pattern for high-performance Python (uvloop)
- Fallback to to_thread for unsupported event loops

### 4. Rust-Side Correlation Tracking

**Decision**: Move correlation ID tracking from Python to Rust.

**Rationale**:
- Enables single GIL acquisition per request/response cycle
- Rust parses headers and dispatches by correlation ID
- DashMap for thread-safe concurrent access
- Python only serializes/deserializes payload

## Performance Targets

| Metric | Baseline (Measured) | Target |
|--------|---------------------|--------|
| Sync p99 (immediate) | 2.0us | <10us (PASS) |
| Sync p99 (blocked) | 62us | <15us |
| Async p99 (to_thread) | 200us | <20us |
| Hot path allocations | 35 bytes/msg | 0 |
| 64x concurrent p99 | 5750us | <20us |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| SlotView lifetime management | Guard with timeout, copy fallback |
| add_reader incompatibility | Detect + to_thread fallback |
| Cross-platform eventfd | Already handled in notify.rs |
| GIL contention | Batch collection, allow_threads |

## Critical Files

### Files to Modify

| File | Change |
|------|--------|
| `src/rs/vertebra-core/src/notify.rs` | Add spin_wait_or_park |
| `src/rs/vertebra-core/src/channel.rs` | Add SlotView, recv_to_slot |
| `src/rs/vertebra-py/src/lib.rs` | Add PySlotView, FastChannel |
| `src/py/vertebra/ipc/channel.py` | Add recv_fast, recv_to_view |
| `src/py/vertebra/resources/broker.py` | Use add_reader pattern |

### Files Created

| File | Purpose | Status |
|------|---------|--------|
| `benchmarks/ipc_fast_path.py` | Fast path benchmark suite | Done |
| `src/py/tests/unit/ipc/test_fast_path.py` | Fast path unit tests | Pending |
| `src/py/tests/integration/ipc/test_uvloop.py` | uvloop integration tests | Pending |

## Consensus Summary

### Models Consulted
- Gemini 3 Pro (FOR stance): 9/10 confidence
- Gemini 3 Pro (AGAINST stance): 8/10 confidence

### Key Agreements
1. Hybrid spin-then-park is MANDATORY
2. asyncio.to_thread elimination is highest priority
3. Zero-copy via memoryview preferred
4. Protocol headers must move to Rust

### Disagreement
- FOR: <10us achievable
- AGAINST: Realistic floor 15-25us for async

### Synthesis
8.5/10 confidence that sync path achieves <10us, async path achieves <20us

## Next Steps

Run `/implement rust-fast-path` to begin implementation.

Start with Phase 1 (Hybrid Spin-Wait) as it's the foundation for all other phases.
