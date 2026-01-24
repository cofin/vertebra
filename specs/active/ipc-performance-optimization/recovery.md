# Recovery Guide: World-Class IPC Performance Optimization

## Quick Resume

- **Last Phase**: Planning Complete
- **Current Status**: Ready for Implementation
- **PRD Location**: `specs/active/ipc-performance-optimization/`

## Intelligence Context

### Complexity
High - Multi-language architecture change across Python, Rust, and TypeScript

### Stack
- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **Build**: Make-based with language-specific tooling

### Commands

| Action | Command |
|--------|---------|
| Install | `make install` |
| Test (Python) | `make test` |
| Test (TypeScript) | `make js-test` |
| Test (Rust) | `make rs-test` |
| Test (All) | `make test-all` |
| Lint | `make lint-all` |
| Single Python test | `uv run pytest src/py/tests/path/to/test.py -v` |
| Single TS test | `cd src/ts && bun test path/to/test.ts` |
| Single Rust test | `cd src/rs && cargo test --package vertebra-core test_name` |

### Documentation
- System: Markdown in `docs/`
- Location: `docs/architecture/`
- PRD Location: `specs/active/`

### Similar Implementations Referenced

1. `src/rs/vertebra-core/src/channel.rs` - SPSC ring buffer patterns
2. `src/rs/vertebra-core/src/notify.rs` - Notification primitives (modify)
3. `src/py/vertebra/ipc/async_transport.py` - Async event loop integration
4. `src/ts/src/broker/server.ts` - Broker polling (replace)
5. `src/rs/vertebra-py/src/supervisor.rs` - Process supervisor (modify)

### Patterns Being Followed

- **FD Inheritance**: Create eventfd before fork, pass FD numbers via env vars
- **Zero-Copy Buffers**: Pre-allocated pools, memoryview returns
- **Event-Driven**: asyncio.add_reader() for async, select.select() for sync
- **Cache-Line Alignment**: 64-byte alignment to prevent false sharing

## State Summary

### Completed

- [x] Problem analysis (8 copies per round-trip identified)
- [x] Multi-model consensus (Gemini 3 Pro, GPT 5.2, Gemini CLI)
- [x] Critical flaw identified (eventfd sharing via EFD_CLOEXEC)
- [x] Solution selected (Process Inheritance)
- [x] Research document (2460 words)
- [x] PRD document (3448 words)
- [x] Task breakdown (24 tasks, 4 phases)
- [x] World-class targets defined (p99 less than 10us, greater than 1M msg/sec)

### Pending

- [ ] Phase 1: Zero-Copy Foundation + FD Inheritance
- [ ] Phase 2: Event-Driven Wakeups
- [ ] Phase 3: FlatBuffers Control Plane
- [ ] Phase 4: Request Multiplexing + Polish
- [ ] Benchmark validation
- [ ] Documentation updates

## Key Design Decisions

### 1. Process Inheritance (NOT Named Pipes)

**Decision**: Use process inheritance for eventfd sharing, not named pipes.

**Rationale**:
- Best performance (eventfd is faster than FIFO)
- No filesystem paths needed
- Cleaner architecture (supervisor owns FD lifecycle)
- User explicitly requested this approach regardless of complexity

### 2. Both Sync AND Async Optimized

**Decision**: Optimize both sync and async paths to achieve same targets.

**Rationale**:
- User explicitly required sync support
- Sync path uses direct select.select(), not thread pool
- Async path uses asyncio.add_reader()
- Both achieve sub-10us p99

### 3. FlatBuffers for Control Plane Only

**Decision**: Use FlatBuffers for headers, keep MessagePack for dynamic payloads.

**Rationale**:
- Control plane schema is known at compile time
- Dynamic payloads (user data, SQL results) have unknown schema
- Hybrid approach gives best of both worlds

### 4. Zero Allocations in Hot Path

**Decision**: Pre-allocate all buffers, return memoryview slices.

**Rationale**:
- Eliminates GC pressure
- Consistent latency (no allocation spikes)
- Required for world-class performance

## Critical Files

### Must Modify

| File | Change | Priority |
|------|--------|----------|
| `src/rs/vertebra-core/src/notify.rs` | Add InheritableNotifier | Phase 1 |
| `src/rs/vertebra-py/src/supervisor.rs` | Create FDs before fork | Phase 1 |
| `src/ts/src/broker/shm.ts` | Add BufferPool | Phase 1 |
| `src/ts/src/broker/server.ts:53-55` | Remove setInterval | Phase 2 |
| `src/py/vertebra/server/worker.py:232` | Remove asyncio.sleep | Phase 2 |
| `src/py/vertebra/ipc/channel.py` | Optimize sync methods | Phase 2 |

### Must Create

| File | Purpose | Priority |
|------|---------|----------|
| `src/schema/ipc.fbs` | FlatBuffers schema | Phase 3 |
| `src/rs/vertebra-core/src/protocol/` | Rust FlatBuffers | Phase 3 |
| `src/py/vertebra/protocol/flatbuffers/` | Python FlatBuffers | Phase 3 |
| `src/ts/src/protocol/flatbuffers/` | TS FlatBuffers | Phase 3 |

## Performance Targets

| Metric | Current | Target |
|--------|---------|--------|
| p99 latency | 1-2ms | less than 10us |
| Throughput | ~100K/sec | greater than 1M/sec |
| Hot path allocs | 1 per msg | Zero |
| Idle CPU | 1-5% | less than 0.1% |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| FD inheritance complexity | Test with strace on Linux |
| Bun event loop integration | NAPI TSFN fallback available |
| FlatBuffers effort | Schema-first design |
| Platform parity | Linux primary, others fallback |

## Next Steps

Run `/implement ipc-performance-optimization` to begin implementation.

Alternatively, implement manually starting with Phase 1:
1. Modify `notify.rs` to add `InheritableNotifier`
2. Modify `supervisor.rs` to create FDs before fork
3. Add `BufferPool` to `shm.ts`
4. Run `make test-all` to verify no regressions
