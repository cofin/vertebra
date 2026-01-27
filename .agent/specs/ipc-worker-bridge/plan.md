# Plan: IPC Worker Bridge

## Phase 1: Worker Manager Structure
- [x] **Task: Scaffold `WorkerManager` in `vertebra-server`** 3f0e18a
    - Create `src/rs/vertebra-server/src/worker.rs`.
    - Define `WorkerManager` struct.
    - Add dependency on `vertebra-core` (path).

## Phase 2: IPC Setup logic
- [x] **Task: Implement Ring Creation** edb35f8
    - In `WorkerManager::start()`, loop `N` times (for N workers).
    - For each worker, create `SpscRing` pair (Request/Response) using `ShmRegion`.
    - Store the server-side handles (`SpscSender` / `SpscReceiver`).

## Phase 3: Process Spawning
- [x] **Task: Port Supervisor Logic** edb35f8
    - Adapt logic from `vertebra-py/src/supervisor.rs` to `vertebra-server/src/worker.rs`.
    - Use `std::process::Command` to spawn `sys.executable` (passed in or detected).
    - Set `VERTEBRA_NOTIFY_FD`, `VERTEBRA_SHM_FD` env vars.
    - Ensure `FD_CLOEXEC` is NOT set for these FDs (handled by `InheritableNotifier`).

## Phase 4: Integration
- [x] **Task: Expose to `VertebraServer`** edb35f8
    - Integrate `WorkerManager` into `VertebraServer` struct.
    - Ensure `VertebraServer::serve()` starts the workers.
- [~] **Task: Verify Worker Startup**
    - Create a test that starts `VertebraServer`, which spawns a "dummy" Python worker (simple script) that verifies it received the FDs.
