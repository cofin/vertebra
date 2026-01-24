# Rust Fast Path Analysis

## Multi-Model Consensus Results

### Models Consulted
- Gemini 3 Pro (FOR stance): 9/10 confidence
- Gemini 3 Pro (AGAINST stance): 8/10 confidence

### Key Agreements (High Confidence)

1. **Hybrid spin-then-park is MANDATORY**
   - Pure eventfd wake has 3-5us latency
   - Must spin for 2-5us before blocking to catch immediate responses
   - Configurable via VERTEBRA_SPIN_US environment variable

2. **asyncio.to_thread elimination is highest priority**
   - Current overhead: ~20us per call
   - Replace with loop.add_reader(eventfd_fd)
   - Single biggest optimization available

3. **Zero-copy via memoryview preferred over buffer indices**
   - Buffer index API degrades developer experience
   - Use PyO3 buffer protocol (__getbuffer__/__releasebuffer__)
   - Leverage existing BufferView pattern in codebase

4. **Protocol headers must move to Rust**
   - Currently parsed in Python (channel.py lines 44-60)
   - Rust can extract correlation ID without Python involvement
   - Enables single GIL acquisition per request/response cycle

5. **eventfd integration is standard pattern**
   - Already used by uvloop, similar high-perf libraries
   - InheritableNotifier already exists in notify.rs
   - Cross-platform fallback (pipes) already implemented

### Points of Disagreement

| Aspect | FOR Stance | AGAINST Stance |
|--------|-----------|----------------|
| Achievable p99 | <10us | 15-25us floor |
| Sync path | Definitely achievable | Achievable |
| Async path | Achievable with care | Python runtime limits |
| GC impact | Manageable | Hard floor on p99 |

### Synthesized Three-Tier Target

```
Tier 1: Sync fast path (Rust-level)     -> <10us p99  [HIGH CONFIDENCE]
Tier 2: Async optimized (add_reader)    -> <20us p99  [MEDIUM CONFIDENCE]
Tier 3: Current baseline                -> ~55us p99  [MEASURED]
```

## Bottleneck Analysis

### Current Hot Path (55us)
```
Python call
  |-- GIL acquisition (~2us)
  |-- PyO3 dispatch (~1us)
  |-- Rust recv (~1us)
  |-- PyBytes::new allocation (~3us)
  |-- GIL release (~1us)
  |-- Python object handling (~5us)
  |-- asyncio.to_thread overhead (~20us)
  |-- Thread context switch (~10us)
  |-- Second GIL acquisition (~2us)
  \-- Result dispatch (~10us)
```

### Optimized Hot Path (Target: <10us)
```
Python call
  |-- Single GIL acquisition (~2us)
  |-- PyO3 dispatch (~1us)
  |-- Rust spin-wait (0-5us)
  |-- Rust recv to slot view (~1us)
  |-- Return memoryview (~1us)
  \-- GIL release (~1us)
```

## Codebase Foundation

### Existing Assets

1. **InheritableNotifier** (`notify.rs:276-574`)
   - Already has `wait(timeout_ms)` method
   - Already handles cross-platform (eventfd vs pipes)
   - Already used by NativeSupervisor

2. **BufferView** (`channel.py:87-165`)
   - Already returns memoryview
   - Already has context manager support
   - Pattern can be replicated in Rust

3. **Protocol Headers** (`src/rs/vertebra-core/src/protocol/`)
   - Already has RequestHeader/ResponseHeader structs
   - Need to add parse_from_bytes methods

4. **PyBuffer with buffer protocol** (`lib.rs:258-613`)
   - Already implements __getbuffer__/__releasebuffer__
   - Pattern to follow for SlotView

### New Components Needed

1. **spin_wait_or_park()** in InheritableNotifier
2. **SlotView** struct with ring buffer lifetime tracking
3. **PySlotView** with buffer protocol
4. **FastChannel** with Rust-side correlation tracking
5. **async recv** using add_reader pattern

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| SlotView lifetime | Medium | High | Arc<RingGuard> blocking |
| add_reader compat | Low | Medium | Detect + fallback |
| Cross-platform | Low | Low | Already handled |
| GIL contention | Medium | Medium | Batch collection |

## Recommendation

**PROCEED** with 4-phase implementation:
- Phase 1: Hybrid spin-wait (foundation)
- Phase 2: Zero-copy SlotView
- Phase 3: asyncio.add_reader
- Phase 4: Rust correlation tracking

Confidence: 8.5/10 that sync path achieves <10us, async path achieves <20us.
