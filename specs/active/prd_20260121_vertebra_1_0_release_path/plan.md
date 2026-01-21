# Implementation Plan: Vertebra 1.0 Release Path

## Phase 1: Stabilization & Data Plane (SQLSpec/Arrow)
**Goal:** Ensure the foundation is leak-free, correctly named, and supports high-performance Arrow transport.

- [x] Task: Finish SQLSpec Public API Rename (Broker* → Vertebra*)
    - [x] Rename classes in `_config.py` and `_driver.py`
    - [x] Update dialect-specific configs (Postgres, MySQL, SQLite)
    - [x] Update all internal and external test imports
- [x] Task: Implement Msgspec Codec Hardening
    - [x] Write tests for round-trip serialization of Date, UUID, and Enum
    - [x] Fix identified edge cases in msgspec encoding/decoding logic
- [x] Task: Implement Arrow IPC Integration
    - [x] Write tests for threshold-based transport switching (MessagePack vs Arrow)
    - [x] Implement Arrow threshold configuration in `_config.py`
    - [x] Implement Arrow encoding in `_driver.py` using `nanoarrow`
    - [x] Implement Arrow decoding in `_shared.py`
- [x] Task: Hardened Shared Memory Cleanup
    - [x] Write stress tests that simulate process crashes
    - [x] Implement a robust cleanup mechanism in the supervisor/test-fixtures that unlinks SHM even on SIGKILL
- [x] Task: Checkpoint - Verify Phase 1 complete and create recovery point [checkpoint: a7eea60]
- [x] Task: Flow - User Manual Verification 'Phase 1: Stabilization' (Protocol in workflow.md)

## Phase 2: Distribution Toolchain & Rust Supervisor
**Goal:** Deliver a robust process manager and a universal build system capable of producing single-file binaries.

- [x] Task: Implement Rust Supervisor
    - [x] Write unit tests for process lifecycle (spawn, monitor, kill)
    - [x] Implement `Supervisor` struct in `src/rs/vertebra-py/src/supervisor.rs`
    - [x] Ensure GIL is released during process waits to prevent worker starvation
    - [x] Implement Unix signal handling (SIGTERM/SIGINT) for graceful shutdown
- [x] Task: Bun Engine Compilation & Target Mapping
    - [x] Implement `build-engine.ts` to produce target-specific Bun binaries
    - [x] Map targets for Linux/macOS (x64/arm64)
- [x] Task: Implement PBS Management (`vertebra.build.standalone`)
    - [x] Create `src/py/vertebra/build/standalone.py`
    - [x] Implement `download_standalone(target, profile, cache_dir)` using `httpx`
    - [x] Define `STANDALONE_RELEASES` mapping for x64/arm64/modern/baseline
    - [x] Add tests for download, caching, and checksum verification
- [x] Task: Implement Single-File Bundler (`vertebra.build.bundler`)
    - [x] Create `src/py/vertebra/build/bundler.py`
    - [x] Implement logic to extract PBS and layout the build directory
    - [x] Implement dependency resolution (installing user deps into bundle)
    - [x] Bundle the correct `vertebra-engine` binary
- [x] Task: Update CLI for Build Commands
    - [x] Update `src/py/vertebra/cli.py` to add `vertebra build` group
    - [x] Add `vertebra build --one-file` command
    - [x] Wire up CLI to `vertebra.build.bundler`
- [x] Task: Checkpoint - Verify Phase 2 complete and create recovery point [checkpoint: checkpoint/prd_20260121_vertebra_1_0_release_path/3]
- [ ] Task: Flow - User Manual Verification 'Phase 2: Distribution' (Protocol in workflow.md)

## Phase 3: Full-Suite MCP Runtime
**Goal:** Expose Python logic as MCP tools across Stdio, SSE, and WebSocket transports.

- [x] Task: MCP Server Foundation (Stdio)
    - [x] Implement JSON-RPC over Stdio transport in Bun
    - [x] Add `initialize`, `tools/list`, and `tools/call` handlers
- [x] Task: Python MCP Bridge (@mcp_tool)
    - [x] Write tests for `@mcp_tool` registration and metadata extraction
    - [x] Implement automatic JSON schema generation from Python type hints
    - [x] Implement the IPC bridge to route tool calls to Python workers via ring buffers
- [x] Task: MCP Web Transports (SSE & WebSocket)
    - [x] Write integration tests for SSE streaming and WebSocket bidirectional messaging
    - [x] Implement SSE transport in `src/ts/src/mcp/transports/sse.ts`
    - [x] Implement WebSocket transport in `src/ts/src/mcp/transports/websocket.ts`
- [ ] Task: Checkpoint - Verify Phase 3 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 3: MCP Runtime' (Protocol in workflow.md)

## Phase 4: Verification & Documentation
**Goal:** Final quality gates and essential guides for users.

- [ ] Task: Document macOS/Windows Signing Processes
    - [ ] Research and document macOS notarization requirements for bundled binaries
    - [ ] Document Windows code signing procedures
    - [ ] Create `docs/distribution/signing.md`
- [ ] Task: Performance Benchmarking
    - [ ] Execute benchmarks comparing MessagePack vs Arrow latency/throughput
    - [ ] Document results in `docs/architecture/performance-tuning.md`
- [ ] Task: Final 1.0 Quality Gate Pass
    - [ ] Run full `make test-all` and `make lint-all`
    - [ ] Verify 50 consecutive test runs pass with zero SHM leaks
- [ ] Task: Checkpoint - Verify Phase 4 complete and create recovery point
- [ ] Task: Flow - User Manual Verification 'Phase 4: Final 1.0 Prep' (Protocol in workflow.md)
