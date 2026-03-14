# Research: Project Performance Goals

## Executive Summary
- **Targets Found:** The project has explicit microsecond-level latency targets (p99 <10µs for sync, <20µs for async) and throughput goals (>1M/sec).
- **Current Status:** Documentation indicates these targets are currently being met or exceeded (e.g., Async p99 at 2.0µs vs <20µs target).
- **Core Philosophy:** "Performance is non-negotiable." Zero-copy (Arrow/Shared Memory) is mandated over serialization where possible.
- **Optimization Strategy:** Dual-serialization approach using MessagePack for small payloads (<1k rows) and Apache Arrow for large datasets (>1k rows).

## Codebase Analysis

### Relevant Modules
- **`docs/architecture/performance-tuning.md`**: The primary source of truth for performance targets and benchmarks.
- **`docs/architecture/rust-ipc-core.md`**: Details the low-level ring buffer implementation driving these metrics.
- **`src/py/vertebra/_config.py`**: Contains tunable thresholds (e.g., `ARROW_ROW_THRESHOLD`).

### Existing Patterns
- **Hybrid Spin-Wait**: A 3-phase receive strategy (Immediate -> Spin -> Park) to minimize syscalls.
- **Zero-Copy IPC**: SPSC/MPMC ring buffers for data transfer.
- **Dual Serialization**: Automatic switching between msgspec and Arrow based on payload size.

### Constraints
- **Strict Latency Budgets**: Any changes to the IPC layer must not degrade the p99 latencies below the established targets.
- **Arrow Dependency**: Large data handling heavily relies on Apache Arrow; alternatives must prove superior performance.

## Documentation Review

### Performance Targets (from `docs/architecture/performance-tuning.md`)

| Metric | Target | Achieved | Notes |
|--------|--------|----------|-------|
| Sync p99 (immediate) | <10µs | **1.15µs** | Data already in ring buffer |
| Sync p99 (blocked) | <15µs | **0.8µs** | Futex wakeup latency |
| Async p99 | <20µs | **2.0µs** | With asyncio integration |
| Multiplexed (64x) | <20µs | **1.7µs** | Concurrent correlation tracking |
| Throughput | >1M/sec | ✓ | Requires batching |

## Prior Art & Recommendations

### Internal References
- The "DB Node" pattern is the established architectural choice for managing these performance characteristics.
- Previous benchmarks (`benchmarks/ipc_fast_path.py`) are used to validate these numbers.

### Recommended Approach
- **Maintain Baselines**: Any new feature or refactor must verify against the standard benchmarks (`make bench` equivalent).
- **Adhere to Philosophy**: Continue prioritizing zero-copy mechanisms.
- **Documentation**: Keep `docs/architecture/performance-tuning.md` updated if targets change.

## Risk Assessment

### Technical Risks
- **Regression**: High probability of performance regression if high-level Python abstractions are introduced without care.
- **Complexity**: The hybrid spin-wait and zero-copy mechanisms introduce significant complexity in the Rust/Python boundary.

### Mitigation
- **Continuous Benchmarking**: Run `benchmarks/ipc_fast_path.py` on critical changes.
- **Strict Review**: Performance-critical code (IPC, hot paths) requires rigorous review.
