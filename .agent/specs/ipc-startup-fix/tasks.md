# IPC Startup Fix - Implementation Tasks

## Phase 1: Shared Memory Cleanup

### Task 1.1: Implement unlink-before-create in Rust ring buffer
- **File**: `src/rs/vertebra-core/src/ring.rs`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `shm_unlink()` called before `shm_open()` in `create_named()`
  - [x] ENOENT from unlink is ignored
  - [x] O_EXCL flag ensures atomic creation
  - [x] Unit test: create with existing file succeeds

### Task 1.2: Add supervisor-level stale shm cleanup
- **File**: `src/rs/vertebra-py/src/supervisor.rs`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `cleanup_stale_shm("vertebra-")` called at supervisor start
  - [x] Iterates `/dev/shm/` for matching prefix
  - [x] Errors on individual files don't stop cleanup
  - [x] Unit test: cleanup removes matching files

## Phase 2: Process Management Consolidation

### Task 2.1: Add VERTEBRA_SKIP_PROCESS_MANAGER env var check
- **File**: `src/ts/src/server/index.ts`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] Check `process.env.VERTEBRA_SKIP_PROCESS_MANAGER === "1"`
  - [x] Skip ProcessManager creation when set
  - [x] IpcConnectionPool still created normally
  - [x] Unit test: ProcessManager not instantiated when env var set

### Task 2.2: Set skip env var in Rust supervisor
- **File**: `src/rs/vertebra-py/src/supervisor.rs`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `.env("VERTEBRA_SKIP_PROCESS_MANAGER", "1")` added to Bun spawn
  - [x] Existing env vars preserved
  - [x] Integration test: only N workers spawn (not 2N)

### Task 2.3: Add parent death signal handling (NEW)
- **File**: `src/rs/vertebra-py/src/supervisor.rs`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `prctl(PR_SET_PDEATHSIG, SIGTERM)` in `pre_exec` for Bun
  - [x] `prctl(PR_SET_PDEATHSIG, SIGTERM)` in `pre_exec` for workers
  - [x] Children die when parent dies unexpectedly (Linux-only)

## Phase 3: Health Validation

### Task 3.1: Implement IPC ping in status handler
- **File**: `src/ts/src/server/status.ts`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `pingWorkerViaIpc()` function added
  - [x] Acquires channel from pool, sends ping scope
  - [x] 2s timeout on ping
  - [x] Returns `ipcStatus: "ready"` only on successful ping
  - [x] Returns `ipcStatus: "error"` on timeout/failure
  - [x] Unit test: ping success/timeout paths

### Task 3.2: Return correct HTTP status codes
- **File**: `src/ts/src/server/status.ts`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] HTTP 200 when `ipcStatus === "ready"`
  - [x] HTTP 503 otherwise
  - [x] Benchmark health check sees 200 after startup
  - [x] Integration test: health returns 503 during startup, 200 after ready

### Task 3.3: Handle vertebra.ping scope in Python workers
- **File**: `src/py/vertebra/server/worker.py`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] Check for `scope["type"] == "vertebra.ping"`
  - [x] Return immediate success response (empty body, status 200)
  - [x] No ASGI app invocation for ping
  - [x] Unit test: ping scope handled correctly

## Phase 4: Broker Status IPC

### Task 4.1: Add status handler to BrokerServer
- **File**: `src/ts/src/broker/server.ts`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] Handle `{ kind: "status" }` message
  - [x] Return `StatusResponse` with bun/broker/ipc sections
  - [x] Include uptime, client count, pool stats
  - [x] Unit test: status response shape correct

### Task 4.2: Implement Python StatusClient
- **File**: `src/py/vertebra/resources/status_client.py` (new)
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `StatusClient` class with `query_status()` method
  - [x] Opens control ring, creates client rings
  - [x] Registers with broker, sends status request
  - [x] Returns dict on success, None on any failure
  - [x] Cleans up rings in finally block
  - [x] Unit test: successful query; timeout returns None

### Task 4.3: Update CLI banner collector
- **File**: `src/py/vertebra/cli/banner/collector.py`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] Remove httpx import
  - [x] Use `StatusClient.query_status()` instead
  - [x] Return empty `ServerStatus` on any failure
  - [x] No exception propagation
  - [x] Unit test: collector handles IPC failure gracefully

## Phase 5: Cleanup

### Task 5.1: Remove httpx from runtime dependencies
- **File**: `pyproject.toml`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] httpx moved to test dependencies (still needed for benchmark utils)
  - [x] standalone.py updated to use urllib instead of httpx
  - [x] `uv lock` succeeds
  - [x] `make test` passes

### Task 5.2: Remove unused ProcessManager paths
- **Files**: `src/ts/src/server/process-manager.ts`, `src/ts/src/server/index.ts`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] ProcessManager only used when `VERTEBRA_SKIP_PROCESS_MANAGER` not set
  - [x] Dead code paths removed
  - [x] TypeScript compiles without errors

### Task 5.3: Update documentation
- **Files**: `CLAUDE.md`, `docs/` as needed
- **Status**: Pending
- **Acceptance**:
  - [ ] Architecture diagrams updated
  - [ ] Environment variables documented
  - [ ] Troubleshooting section for shm cleanup

### Task 5.4: Archive merged specs
- **Directories**: `specs/active/cli-broker-status-ipc/`
- **Status**: Pending
- **Acceptance**:
  - [ ] Move to `specs/archived/`
  - [ ] Add note referencing this PRD
  - [ ] Update any cross-references

## Phase 6: Benchmark Safety (NEW)

### Task 6.1: Add orphan process cleanup function
- **File**: `benchmarks/servers/utils.py`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] `cleanup_orphaned_vertebra_processes()` function added
  - [x] Kills orphaned vertebra workers via pkill
  - [x] Cleans up stale `/dev/shm/vertebra-*` files
  - [x] Handles errors gracefully

### Task 6.2: Integrate cleanup into benchmark runner
- **File**: `benchmarks/servers/runner.py`
- **Status**: ✅ Complete
- **Acceptance**:
  - [x] Cleanup called at start of benchmark run
  - [x] Cleanup called in finally block
  - [x] Prevents runaway processes from previous crashes

## Validation Checklist

Before marking complete:

- [ ] `make bench-vertebra` completes successfully
- [x] `vertebra serve` starts without "File exists" errors
- [x] Health endpoint returns 200 only when IPC is functional
- [x] CLI banner displays status without httpx
- [x] No duplicate worker processes on startup
- [ ] All unit tests pass (`make test-all`)
- [ ] No regressions in existing functionality
