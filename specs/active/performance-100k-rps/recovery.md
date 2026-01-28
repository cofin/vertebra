# Recovery Guide: 100K-200K RPS Performance Target

## Quick Resume

**Last phase:** PRD Complete
**Current status:** Ready for Implementation
**PRD location:** `specs/active/performance-100k-rps/`

## Intelligence Context

**Complexity:** Complex (10+ checkpoints)

**Stack:**
- Rust: vertebra-core (channel.rs, sync.rs, notify.rs)
- Rust: vertebra-py (PyO3 bindings)
- Python: vertebra.core.worker, vertebra.messaging.broker

**Commands:**
- Test Rust: `cargo test --package vertebra-core`
- Test Python: `uv run pytest src/py/tests`
- Benchmark: `make bench-servers --servers vertebra --scenarios hello,json`
- Build Rust: `cd src/rs && cargo build --release`
- Rebuild PyO3: `cd src/rs/vertebra-py && maturin develop`

**Documentation:**
- System: Sphinx
- Location: `docs/`
- Skills: `~/.claude/skills/vertebra/`

**Similar implementations:**
- Granian (external) - GIL optimization
- io_uring (Linux kernel) - conditional wake
- LMAX Disruptor - batch processing

**Patterns being followed:**
- Conditional wake (only syscall if waiters exist)
- Optimistic GIL (try with GIL, release only if blocking)
- Batch amortization (N items per FFI crossing)
- Relaxed fast-path (Acquire only before payload access)

## State Summary

### Completed
- [x] Python source analysis (bottlenecks identified)
- [x] Rust source analysis (bottlenecks identified)
- [x] Multi-model consensus (Gemini 3 Pro + Flash)
- [x] PRD written (3,400+ words)
- [x] Research documented (2,150+ words)
- [x] Tasks broken down (20 tasks, 3 phases)

### Pending
- [ ] P0 implementation (Tasks 0.1-0.7)
- [ ] P1 implementation (Tasks 1.1-1.7)
- [ ] P2 implementation (Tasks 2.1-2.6)
- [ ] Final validation

## Key Files to Modify

| Priority | File | Change Summary |
|----------|------|----------------|
| P0 | `src/rs/vertebra-core/src/channel.rs` | wait_count, conditional wake |
| P0 | `src/rs/vertebra-py/src/lib.rs` | Optimistic GIL pattern |
| P0 | `src/py/vertebra/messaging/broker.py` | Remove dict copies |
| P1 | `src/rs/vertebra-core/src/channel.rs` | recv_batch, Relaxed ordering |
| P1 | `src/py/vertebra/core/worker.py` | Batch processing |
| P1 | `src/py/vertebra/core/serializers/codecs.py` | Pre-create decoders |

## Critical Implementation Notes

### Conditional Wake (MUST NOT MISS)

The double-check pattern is CRITICAL to prevent lost wakeups:

```rust
// CORRECT ORDER
1. try_recv() fast path
2. wait_count.fetch_add(1, Release)  // Increment FIRST
3. try_recv() again  // Double-check AFTER increment
4. notifier.wait()  // Only if still empty
5. wait_count.fetch_sub(1, Relaxed)
```

If this order is wrong, messages WILL be lost under load.

### Memory Ordering (MUST NOT MISS)

When relaxing to `Relaxed`, MUST add Acquire fence before payload access:

```rust
// WRONG - Data race!
let tail = self.write_head().load(Ordering::Relaxed);
let data = &buffer[..];  // May see stale data!

// CORRECT
let tail = self.write_head().load(Ordering::Relaxed);
std::sync::atomic::fence(Ordering::Acquire);  // REQUIRED
let data = &buffer[..];  // Now safe
```

### PyO3 GIL Pattern (MUST NOT MISS)

Fast path MUST try with GIL first:

```rust
// WRONG - Always toggles GIL
let len = py.allow_threads(|| self.ring.recv(&mut buf))?;

// CORRECT - Only toggles if needed
match self.ring.try_recv(&mut buf) {
    Ok(len) => return Ok(len),  // Fast path, no GIL toggle
    Err(Empty) => {
        py.allow_threads(|| self.ring.recv(&mut buf))  // Slow path
    }
}
```

## Benchmark Baselines

Before any changes, record:

```bash
make bench-servers --servers vertebra,granian --scenarios hello,json \
  > specs/active/performance-100k-rps/tmp/baseline.json
```

Expected baseline:
- Vertebra hello: ~12K RPS
- Vertebra json: ~10K RPS
- Granian hello: ~65K RPS (reference)

## Rollback Plan

If changes cause issues:

1. **Feature flag:** All P0 changes can be behind `#[cfg(feature = "perf-opt")]`
2. **Git revert:** Each task should be a separate commit
3. **Binary switch:** Keep old binary for A/B testing

## Next Steps

1. Run `/implement performance-100k-rps` to begin implementation
2. Start with Task 0.1 (add wait_count)
3. Run benchmarks after each task to validate improvement
4. Proceed through P0 → P1 → P2

## Contact Points

- **PRD:** `specs/active/performance-100k-rps/prd.md`
- **Tasks:** `specs/active/performance-100k-rps/tasks.md`
- **Analysis:** `specs/active/performance-100k-rps/research/analysis.md`
- **Patterns:** `.agent/patterns.md`
- **Related PRDs:** `.agent/specs/async-ipc-pipelining/prd.md`
