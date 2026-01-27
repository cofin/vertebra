# Plan: IPC Worker Bridge

## Phase 1: Worker Manager Structure
- [ ] **Task: Scaffold `WorkerManager` in `vertebra-server`**
    - Create `src/rs/vertebra-server/src/worker.rs`.
    - Define `WorkerManager` struct.
    - Add dependency on `vertebra-core` (path).

## Phase 2: IPC Setup logic
- [ ] **Task: Implement Ring Creation**
    - In `WorkerManager::start()`, loop `N` times (for N workers).
    - For each worker, create `SpscRing` pair (Request/Response) using `ShmRegion`.
    - Store the server-side handles (`SpscSender` / `SpscReceiver`).

## Phase 3: Process Spawning
- [ ] **Task: Port Supervisor Logic**
    - Adapt logic from `vertebra-py/src/supervisor.rs` to `vertebra-server/src/worker.rs`.
    - Use `std::process::Command` to spawn `sys.executable` (passed in or detected).
    - Set `VERTEBRA_NOTIFY_FD`, `VERTEBRA_SHM_FD` env vars.
    - Ensure `FD_CLOEXEC` is NOT set for these FDs (handled by `InheritableNotifier`).

## Phase 4: Integration
- [ ] **Task: Expose to `VertebraServer`**
    - Integrate `WorkerManager` into `VertebraServer` struct.
    - Ensure `VertebraServer::serve()` starts the workers.
- [ ] **Task: Verify Worker Startup**
    - Create a test that starts `VertebraServer`, which spawns a "dummy" Python worker (simple script) that verifies it received the FDs.
