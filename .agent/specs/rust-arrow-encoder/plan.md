# Plan: Rust-Based Arrow Encoder (Optimization)

This plan implements a high-performance Arrow IPC encoder in Rust within `vertebra-py` to replace the existing Python row-to-column pivot logic.

## Phase 1: Infrastructure & Scaffolding
Goals: Set up the Rust environment, dependencies, and the bridge for schema metadata.

- [ ] Task: Update `src/rs/vertebra-py/Cargo.toml` to include `arrow` and `arrow-ipc` crates
- [ ] Task: Create `src/rs/vertebra-py/src/arrow.rs` and register it in `src/rs/vertebra-py/src/lib.rs`
- [ ] Task: Implement `ArrowFieldMap` struct in Rust to receive schema metadata from Python
- [ ] Task: Write Failing Tests: Verify Python can call a no-op `encode_arrow_rows` and receive a `PyErr` for unsupported types
- [ ] Task: Checkpoint - Verify Phase 1 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Infrastructure & Scaffolding' (Protocol in workflow.md)

## Phase 2: Core Rust Encoder Implementation
Goals: Implement the actual row-to-column conversion and Arrow IPC stream generation.

- [ ] Task: Implement `encode_arrow_rows` core loop: Iterate over Python objects and use builders for primitive types (Int, Float, Bool, String)
- [ ] Task: Implement `PyDateTime` extraction for high-performance Date/Time column building
- [ ] Task: Write Failing Tests: Verify Correctness for `list[dict]` and `list[tuple]` with mixed types
- [ ] Task: Implement `list[dict]` and `list[tuple]` extraction logic in Rust
- [ ] Task: Write Failing Tests: Verify Correctness for Typed Objects (msgspec Structs, Pydantic)
- [ ] Task: Implement Attribute-based extraction (`PyAny::getattr`) for Typed Objects
- [ ] Task: Checkpoint - Verify Phase 2 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Core Rust Encoder Implementation' (Protocol in workflow.md)

## Phase 3: Python Integration & Optimization
Goals: Switch the production `encode_arrow` path to use Rust and verify the 5x speedup.

- [ ] Task: Update `src/py/vertebra/resources/arrow.py` to use `vertebra._vertebra.encode_arrow_rows`
- [ ] Task: Implement schema resolution using `TypeExtractor` in `encode_arrow` to generate the "Field Map" for Rust
- [ ] Task: Write Failing Tests: Integration tests for the full `encode_arrow` pipeline with various object models
- [ ] Task: Benchmark: Run `benchmarks/arrow_vs_msgspec.py` to verify the >5x performance gain
- [ ] Task: Checkpoint - Verify Phase 3 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Python Integration & Optimization' (Protocol in workflow.md)
