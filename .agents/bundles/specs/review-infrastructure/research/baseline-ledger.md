# Baseline Review Ledger

Full-tree baseline review ledger for `feat/stack-followups` triaged against `findings.md`.

---

### RT-05 · critical · src/rs/vertebra-core/src/channel.rs:L280-L310
- **Finding**: MPMC ring try_recv does not advance consumer slot sequence on empty ring.
- **Evidence**: `channel.rs` sequence advancement is bypassed on empty check leading to waiter starvation.
- **Fix**: Advance consumer slot sequence atomically in `ring/mpmc.rs` (Task 2.4 / Task 8.2).
- **Confidence**: verified
- **Status**: accepted

### HTTP-01 · important · src/rs/vertebra-http/src/lib.rs:L1-L40
- **Finding**: Ingress dispatch pipeline lacks modular middleware stage isolation.
- **Evidence**: Direct handler dispatch mixes authentication, TLS offload, and routing in monolithic block.
- **Fix**: Extract `ingress::Pipeline` with security, transform, and observability stages (Task 3.1).
- **Confidence**: verified
- **Status**: accepted

### FFI-01 · critical · src/rs/vertebra-py/src/request.rs:L140-L170
- **Finding**: Zero-copy body view lacks explicit batch snapshot lifetime guarantee.
- **Evidence**: `PyBuffer` memoryview must hold `Arc<BatchSnapshot>` reference to prevent slab reuse during turn.
- **Fix**: Tie memoryview buffer lifetime to BatchSnapshot slot (Task 4.2 / Task 8.5).
- **Confidence**: verified
- **Status**: accepted

### PY-01 · important · src/py/vertebra/server/__init__.py:L1-L10
- **Finding**: Worker loop lacks clean lane segregation between compute and IO tasks.
- **Evidence**: Single event loop multiplexes heavy CPU tasks and network streaming.
- **Fix**: Split worker lanes into dedicated request dispatch and streaming lanes (Task 4.7).
- **Confidence**: verified
- **Status**: accepted

### PERF-01 · important · tools/benchmarks/targets.py:L95-L100
- **Finding**: 3.14t benchmark fallback silently ran echo command instead of raising error.
- **Evidence**: Missing free-threaded binary resulted in false-positive exit 0 in CI benchmarks.
- **Fix**: Raise FileNotFoundError when 3.14t binary is absent (Task 1.6).
- **Confidence**: verified
- **Status**: accepted
