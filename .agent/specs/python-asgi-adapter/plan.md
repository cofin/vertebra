# Plan: Python ASGI Adapter

## Phase 1: Worker Startup Logic
- [x] **Task: Update `AsgiWorker.start`** 11fad81
    - Detect if running under `vertebra-server` (e.g. check for `VERTEBRA_SHM_PREFIX` or specific env var).
    - If managed: `AsyncRing.open_named` instead of `create_named`.
    - Ensure `shm_unlink` is NOT called by the worker on exit (Server owns the shm).

## Phase 2: Protocol Validation
- [~] **Task: Verify Header Parsing**
    - Review `RequestHeader` struct in `vertebra-core` vs `worker.py`.
    - Ensure byte alignment matches.
- [~] **Task: ASGI Scope Construction**
    - Ensure `vertebra-server` sends all necessary HTTP headers/metadata for valid ASGI scope.
    - Update `RequestMeta` if new fields are needed (e.g., `http_version` from Hyper).

## Phase 3: RSGI Support (Optimization)
- [ ] **Task: Evaluate RSGI**
    - If `vertebra-server` can speak RSGI-over-IPC, implement `RsgiWorker` or add RSGI support to `AsgiWorker`.
    - *Decision:* Stick to native Vertebra protocol for now to keep "Zero-Copy" ring buffer logic consistent.

## Phase 4: Testing
- [ ] **Task: Mock Server Test**
    - Create a Python script that simulates the `vertebra-server` (creates rings, spawns worker).
    - Verify worker attaches and processes a request.
