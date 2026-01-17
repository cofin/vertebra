# PRD-1 Session Recovery Guide

**Purpose**: Enable resumption of PRD-1 implementation across sessions.
**Last Updated**: 2026-01-17

---

## Quick Start

If you need to resume work on PRD-1 Rust Core IPC Layer:

1. **Read the PRD**: `/home/cody/code/c/vertebra/docs/prd/PRD-1-rust-ipc.md` (3200+ words)
2. **Check Tasks**: `/home/cody/code/c/vertebra/docs/prd/PRD-1-rust-ipc/tasks.md` (225+ tasks)
3. **Review Research**: `/home/cody/code/c/vertebra/docs/prd/PRD-1-rust-ipc/research/plan.md` (2400+ words)
4. **Locate Code**: `/home/cody/code/c/vertebra/src/rs/` (implementation directory)

---

## Project Context

### What is PRD-1?

PRD-1 implements a **Rust-based core IPC layer** for the Vertebra project, providing:
- POSIX shared memory management
- Lock-free channels (SPSC/MPMC)
- Pre-allocated buffer pools
- Cross-process synchronization primitives
- C ABI FFI for Python integration
- Apache Arrow IPC support

### Goals

- **Performance**: < 1μs latency for small messages
- **Safety**: Memory safety via Rust ownership + Miri validation
- **Coverage**: 90%+ test coverage
- **Integration**: Seamless Python FFI

### Status

**Current Phase**: Planning Complete
**Next Phase**: Implementation Phase 1 (Foundation & Build System)

---

## File Locations

### Documentation

```
/home/cody/code/c/vertebra/docs/prd/
├── PRD-1-rust-ipc.md              # Main PRD (3200+ words)
└── PRD-1-rust-ipc/
    ├── research/
    │   └── plan.md                # Research findings (2400+ words)
    ├── tasks.md                   # Implementation checklist (225+ tasks)
    ├── recovery.md                # This file
    └── tmp/
        └── (working notes)
```

### Source Code

```
/home/cody/code/c/vertebra/src/rs/
├── Cargo.toml                     # Workspace manifest (to be created)
├── vertebra-core/                 # Core Rust library (to be created)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── shm.rs                 # Shared memory
│       ├── channel.rs             # Lock-free channels
│       ├── buffer.rs              # Buffer pool
│       └── sync.rs                # Synchronization primitives
└── vertebra-ffi/                  # C ABI exports (to be created)
    ├── Cargo.toml
    └── src/
        └── lib.rs
```

### Build System

- **Makefile**: `/home/cody/code/c/vertebra/Makefile` (add Rust targets)
- **CI**: GitHub Actions or equivalent (add Rust validation)

---

## Implementation Phases

### Phase 1: Foundation (Week 1) - NEXT

**Status**: Not Started

**Key Tasks**:
1. Create Cargo workspace structure
2. Initialize vertebra-core and vertebra-ffi crates
3. Configure dependencies (rustix, arrow-rs, etc.)
4. Add Makefile integration
5. Set up CI pipeline

**Start Here**: `/home/cody/code/c/vertebra/docs/prd/PRD-1-rust-ipc/tasks.md` (Phase 1)

### Phase 2: Shared Memory (Week 1-2)

**Deliverables**: ShmRegion implementation with Miri validation

### Phase 3: Lock-Free Channels (Week 2-3)

**Deliverables**: SPSC/MPMC channels with < 1μs latency

### Phase 4: Buffer Pool (Week 3-4)

**Deliverables**: Zero-copy buffer pool with Arrow integration

### Phase 5: Synchronization (Week 4-5)

**Deliverables**: Cross-process futex-based primitives

### Phase 6: FFI Layer (Week 5-6)

**Deliverables**: C ABI + Python bindings

### Phase 7: Integration (Week 6-7)

**Deliverables**: Documentation, examples, final validation

---

## Key Decisions

### Technical Choices

1. **FFI Approach**: C ABI with cffi (not PyO3)
   - Rationale: Minimal surface, stable ABI, no Python runtime deps

2. **Shared Memory**: rustix for POSIX abstractions
   - Rationale: Safe, well-maintained, pure Rust

3. **Channels**: SPSC based on ringbuf patterns, MPMC with epoch-based ABA mitigation
   - Rationale: Proven patterns, cache-friendly, lock-free

4. **Buffer Pool**: Pre-allocated Vec<u8> with slice views
   - Rationale: Predictable memory, O(1) allocation, zero-copy

5. **Synchronization**: Custom futex wrappers without FUTEX_PRIVATE_FLAG
   - Rationale: Cross-process support, fallback to file locks

6. **Arrow IPC**: Phase 1 serialization only, Phase 2 zero-copy
   - Rationale: Progressive enhancement, validate approach first

### Architecture Patterns

- **Workspace Structure**: Separate -core and -ffi crates
- **FFI Safety**: Opaque pointers, no panics across boundary
- **RAII**: Automatic cleanup via Drop trait
- **Testing**: Unit + Integration + Property-based + Miri

---

## Dependencies

### External Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| rustix | 0.38 | POSIX abstractions |
| arrow | 53.0 | Arrow IPC format |
| crossbeam-utils | 0.8 | Atomic utilities |
| thiserror | 2.0 | Error handling |
| criterion | 0.5 | Benchmarking |
| proptest | 1.6 | Property testing |

### System Requirements

- Linux kernel 4.18+
- Python 3.11+
- Rust 1.75+ (stable) + nightly (for Miri)

---

## Common Commands

### Build

```bash
cd /home/cody/code/c/vertebra
make rust-build          # Build Rust components
make rust-test           # Run tests
make rust-miri           # Run Miri (UB detection)
make rust-bench          # Run benchmarks
```

### Development

```bash
cd /home/cody/code/c/vertebra/src/rs

# Build workspace
cargo build --all

# Test specific crate
cargo test -p vertebra-core

# Run Miri
cargo +nightly miri test

# Generate docs
cargo doc --open

# Run benchmarks
cargo bench
```

### CI Validation

```bash
make rust-build
make rust-test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo +nightly miri test
```

---

## Research Summary

Comprehensive research is documented in `/home/cody/code/c/vertebra/docs/prd/PRD-1-rust-ipc/research/plan.md`.

**Key Findings**:

1. **POSIX Shared Memory**: rustix provides safe abstractions, close fd after mmap
2. **Lock-Free Channels**: Cache padding critical for SPSC performance
3. **Arrow IPC**: Zero-copy via memory-mapped files, 64-byte alignment
4. **FFI**: C ABI preferred for minimal surface, PyO3 for high-level APIs
5. **Miri**: Essential for UB detection, combine with valgrind for FFI
6. **Buffer Pools**: Slice-based views with Arc sharing, 5x speedup possible
7. **Futex**: Custom wrapper needed (no FUTEX_PRIVATE_FLAG), fallback to file locks
8. **iceoryx2**: Decentralized architecture, loan-based API, valuable reference
9. **Benchmarking**: Ping-pong pattern, perf + flamegraph, focus on tail latency

**References**: 25+ sources including rustix docs, DEV tutorials, research papers, Medium articles.

---

## Success Criteria

### Performance Targets

- [ ] SPSC latency < 1μs (P95)
- [ ] SPSC throughput > 1M msgs/sec
- [ ] MPMC throughput > 100K msgs/sec
- [ ] Memory overhead < 10%

### Safety Validation

- [ ] All Miri tests pass
- [ ] All valgrind tests pass
- [ ] ThreadSanitizer passes
- [ ] No crashes with invalid inputs

### Testing Coverage

- [ ] Unit tests ≥ 90% coverage
- [ ] Integration tests for all modules
- [ ] Property-based tests for invariants
- [ ] FFI tests from Rust and Python

### Documentation

- [ ] All public APIs documented
- [ ] Getting started guide
- [ ] Integration guide
- [ ] Performance guide
- [ ] API reference (Rust + Python + C)

---

## Risks and Mitigations

### High-Priority Risks

1. **Cross-Process Futex Correctness**
   - Mitigation: Extensive testing, model checking, fallback to file locks

2. **Memory Alignment for Arrow**
   - Mitigation: Static assertions, Miri testing, copy-based fallback

3. **< 1μs Latency Target**
   - Mitigation: Early benchmarking, profiling, contingency: relax to < 5μs

### Medium-Priority Risks

4. **FFI Safety and Memory Leaks**
   - Mitigation: Opaque pointers, Python context managers, valgrind tests

5. **Miri Coverage Limitations**
   - Mitigation: Minimize unsafe code, valgrind for FFI, manual review

---

## Next Steps

### Immediate (Phase 1)

1. Create workspace structure in `/home/cody/code/c/vertebra/src/rs/`
2. Initialize vertebra-core and vertebra-ffi crates
3. Configure Cargo.toml with dependencies
4. Add Makefile targets for Rust builds
5. Set up CI pipeline for Rust validation

### Short-Term (Phase 2-3)

1. Implement ShmRegion with Miri validation
2. Implement SPSC channel with < 1μs latency
3. Create benchmarking infrastructure

### Long-Term (Phase 4-7)

1. Complete all core modules
2. Implement FFI layer
3. Create Python bindings
4. Write comprehensive documentation

---

## Questions to Ask When Resuming

1. **Phase Status**: Which phase are we in? Check tasks.md for progress.
2. **Blockers**: Any technical issues encountered? Check tmp/notes.md.
3. **Dependencies**: Are all external dependencies available?
4. **Tests**: Are existing tests passing?
5. **Documentation**: Is rustdoc up to date?
6. **Performance**: Do benchmarks meet targets?

---

## Tools and Resources

### Development Tools

- **rust-analyzer**: IDE support (VSCode, Emacs, Vim)
- **cargo-watch**: Auto-rebuild on file changes
- **cargo-tarpaulin**: Coverage reporting
- **cargo-llvm-cov**: Alternative coverage tool
- **cargo-flamegraph**: Profiling visualization

### Debugging Tools

- **gdb/lldb**: Debuggers with Rust support
- **perf**: Linux profiling tool
- **valgrind**: Memory error detection
- **Miri**: Undefined behavior detection
- **ThreadSanitizer**: Data race detection

### Documentation

- **Rust Book**: https://doc.rust-lang.org/book/
- **Rustonomicon**: https://doc.rust-lang.org/nomicon/ (unsafe code)
- **rustix docs**: https://docs.rs/rustix/
- **arrow-rs docs**: https://docs.rs/arrow/

---

## Contact and Collaboration

If resuming this work in a new session:

1. Read this recovery guide first
2. Review the main PRD (`PRD-1-rust-ipc.md`)
3. Check task status (`tasks.md`)
4. Review research notes (`research/plan.md`)
5. Start implementing or continue from last checkpoint

---

## Changelog

| Date | Session | Changes |
|------|---------|---------|
| 2026-01-17 | Initial | Created PRD, research, tasks, recovery docs |

---

**Status**: Ready for Implementation
**Next Action**: Begin Phase 1 (Foundation & Build System)
