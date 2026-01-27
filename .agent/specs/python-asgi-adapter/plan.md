# Plan: Python ASGI Adapter

## Phase 1: Worker Startup Logic
- [x] **Task: Update `AsgiWorker.start`** 11fad81
    - Detect if running under `vertebra-server` (e.g. check for `VERTEBRA_SHM_PREFIX` or specific env var).
    - If managed: `AsyncRing.open_named` instead of `create_named`.
    - Ensure `shm_unlink` is NOT called by the worker on exit (Server owns the shm).

## Phase 2: Protocol Validation
- [x] **Task: Verify Header Parsing** 755505e
    - Review `RequestHeader` struct in `vertebra-core` vs `worker.py`.
    - Ensure byte alignment matches.
- [x] **Task: ASGI Scope Construction** 7203ed2
    - Ensure `vertebra-server` sends all necessary HTTP headers/metadata for valid ASGI scope.
    - Update `RequestMeta` if new fields are needed (e.g., `http_version` from Hyper).

## Phase 3: RSGI Support (Optimization)
- [x] **Task: Evaluate RSGI**
    - If `vertebra-server` can speak RSGI-over-IPC, implement `RsgiWorker` or add RSGI support to `AsgiWorker`.
    - *Decision:* Stick to native Vertebra protocol for now to keep "Zero-Copy" ring buffer logic consistent.

## Phase 4: Testing
- [x] **Task: Mock Server Test** 3e2e0be
    - Create a Python script that simulates the `vertebra-server` (creates rings, spawns worker).
    - Verify worker attaches and processes a request.

## Phase 5: Completion
- [ ] **Task: Run Test Suite & Checkpoint**
    - `make test-all`
    - Create tag `checkpoint/python-asgi-adapter/phase-4`
- [ ] **Task: Archive Flow**
    - Move to `.agent/archive/`
