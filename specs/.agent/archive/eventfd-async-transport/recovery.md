# PRD-10: Eventfd Async Transport - Recovery Guide

## Purpose

This document enables any developer (human or AI) to resume work on PRD-10 from any point. It captures the full context, decisions made, and current state.

## Project Context

### What This PRD Solves

Vertebra's Python-to-Bun IPC currently wraps blocking shared-memory operations in `asyncio.to_thread()`, adding 50-130μs overhead per call. This PRD replaces that pattern with native asyncio integration using eventfd signaling.

### Key Technical Decisions

1. **eventfd over polling**: Linux uses eventfd for zero-overhead async notification
2. **PyO3 over CFFI**: CFFI holds GIL during blocking calls; PyO3 with allow_threads() is better but we're going further with eventfd
3. **Buffer pool**: Pre-allocated buffers eliminate 650MB/s allocation churn
4. **No backwards compatibility**: Clean migration, delete CFFI bindings entirely
5. **Polling fallback**: macOS/Windows use single polling thread (acceptable)

### Architecture Summary

```
[Python asyncio loop]
    ↓ loop.add_reader(eventfd)
[epoll waits on eventfd]
    ↓ data available signal
[callback calls try_recv()]
    ↓ non-blocking
[Future resolved with data]
```

## File Inventory

### New Files to Create

| File | Status | Purpose |
|------|--------|---------|
| `src/rs/vertebra-core/src/notify.rs` | Not started | Cross-platform notification (eventfd/pipe) |
| `src/rs/vertebra-core/src/buffer_pool.rs` | Not started | Pre-allocated buffer pool |
| `src/rs/vertebra-core/src/async_ring.rs` | Not started | Async ring with notification |
| `src/rs/vertebra-py/src/async_ring.rs` | Not started | PyO3 bindings |
| `src/py/vertebra/_internal/async_transport.py` | Not started | Python async wrapper |

### Files to Modify

| File | Status | Changes Needed |
|------|--------|----------------|
| `src/rs/vertebra-core/src/lib.rs` | Not started | Add new module exports |
| `src/rs/vertebra-py/src/lib.rs` | Not started | Register PyAsyncRing |
| `src/py/vertebra/resources/broker.py` | Not started | Replace asyncio.to_thread |
| `src/py/vertebra/server/worker.py` | Not started | Replace asyncio.to_thread |
| `src/py/vertebra/_internal/_typing.py` | Not started | Update type stubs |
| Multiple test files | Not started | Update to new API |

### Files to Delete

| File | Status | Reason |
|------|--------|--------|
| `src/py/vertebra/_internal/shm.py` | Not started | CFFI replaced by PyO3 |

## Current State

### What's Done

- [x] Research document written (2000+ words)
- [x] PRD written (3200+ words)
- [x] Task breakdown created (26 tasks)
- [x] This recovery guide

### What's Next

Start Phase 1, Task 1.1: Implement Notification Primitive

## Quick Start for New Session

### 1. Verify Current State

```bash
cd /home/cody/code/c/vertebra
git status  # Should show spec files only, no source changes
make test   # Should pass (87 tests)
```

### 2. Read Key Documents

```
specs/active/eventfd-async-transport/PRD.md      # Full specification
specs/active/eventfd-async-transport/tasks.md   # Task checklist
specs/active/eventfd-async-transport/research.md # Technical analysis
```

### 3. Understand Current Architecture

Key files to read:
- `src/py/vertebra/_internal/shm.py` - Current CFFI bindings (to be deleted)
- `src/rs/vertebra-core/src/sync.rs` - Current wait/wake primitives
- `src/rs/vertebra-py/src/lib.rs` - Current PyO3 bindings (lines 320-440)
- `src/py/vertebra/resources/broker.py` - Example asyncio.to_thread usage

### 4. Start Implementation

Begin with Phase 1, Task 1.1:
```bash
# Create notify.rs
vim src/rs/vertebra-core/src/notify.rs
```

## Implementation Patterns

### Rust Notifier Pattern

```rust
// Linux: eventfd
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };

// macOS: pipe
let mut fds = [0i32; 2];
unsafe { libc::pipe(fds.as_mut_ptr()) };
```

### Python Async Pattern

```python
async def recv(self) -> bytes:
    # Fast path
    data = self._ring.try_recv()
    if data is not None:
        return data

    # Slow path: wait for eventfd
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    self._waiters.append(future)

    if not self._registered:
        loop.add_reader(self._fd, self._on_readable)
        self._registered = True

    return await future
```

### Migration Pattern

```python
# Before
data, _flags = await asyncio.to_thread(self._response.recv, timeout_ms)

# After
data = await self._transport.recv()
```

## Testing Commands

```bash
# Python tests
make test
uv run pytest src/py/tests/test_broker_client.py -v
uv run pytest src/py/tests/test_asgi_worker.py -v

# Rust tests
make rs-test
cd src/rs && cargo test notify
cd src/rs && cargo test buffer_pool
cd src/rs && cargo test async_ring

# Full lint
make lint-all

# Build PyO3 bindings
cd src/rs/vertebra-py && maturin develop
```

## Common Issues

### Issue: eventfd not available

**Symptom**: `libc::eventfd` returns -1 with ENOSYS

**Solution**: Fall back to pipe. Check `errno` and use PollingTransport.

### Issue: Buffer pool exhaustion

**Symptom**: `acquire()` returns None, operations stall

**Solution**:
1. Increase pool size (default: 4, try 8 or 16)
2. Ensure buffers are released after use
3. Add warning when pool is 75% exhausted

### Issue: FD leak

**Symptom**: "Too many open files" error

**Solution**: Ensure `close()` is called on transport. Add `__del__` cleanup.

### Issue: Test using old API

**Symptom**: Test fails with "asyncio.to_thread" or CFFI import error

**Solution**: Update test to use new `AsyncShmTransport` API.

## Key Metrics to Track

| Metric | Before | Target | Measurement |
|--------|--------|--------|-------------|
| IPC latency | 180μs | <100μs | `time.perf_counter()` |
| Allocation rate | 650MB/s | <1MB/s | `tracemalloc` |
| Throughput | 5K req/s | >50K req/s | Load test |

## Contact Points

- **PRD Author**: Claude Code
- **Spec Location**: `specs/active/eventfd-async-transport/`
- **Related PRDs**: PRD-0 (Foundation), PRD-4 (Bun Drivers)

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-19 | 1.0 | Initial PRD, research, tasks, recovery guide |
