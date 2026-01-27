# Spec: IPC Worker Bridge

## Code Analysis Summary
**Current State:**
- `vertebra-core`: Contains `SpscRing`, `ShmRegion`, `Notifier` (EventFD).
- `vertebra-py`: Contains `NativeSupervisor` which spawns Python workers using `Command`.
- `vertebra-server`: (Planned/In-Progress) New crate for the HTTP server.

**Target Architecture:**
- **`vertebra-server`**: Will own the `WorkerManager`.
- **`WorkerManager`**:
    - Creates Shared Memory Regions and Rings.
    - Creates `InheritableNotifier` (EventFD).
    - Spawns Python worker processes, passing FDs via environment variables.
    - Holds the "Server" side of the rings (Sender for Request, Receiver for Response).

## Requirements

### 1. Worker Manager (Rust)
- **Struct:** `WorkerManager` in `vertebra-server`.
- **Functionality:**
    - `new(config: WorkerConfig)`: Initialize configuration.
    - `start()`:
        - Create IPC primitives (Rings, Notifier).
        - Spawn `N` worker processes.
        - Pass `VERTEBRA_NOTIFY_FD`, `VERTEBRA_SHM_FD`, etc. to workers.
    - `get_worker(id)`: Get handle to a specific worker's ring pair.
    - `shutdown()`: Gracefully terminate workers.

### 2. IPC Primitives Integration
- **Rings:** Use `vertebra_core::channel::SpscRing`.
    - One Request Ring per worker (Server -> Worker).
    - One Response Ring per worker (Worker -> Server).
- **Notification:** Use `vertebra_core::notify::InheritableNotifier`.
    - Single EventFD shared (or one per worker) to wake up workers.

### 3. Python Worker Adaptation
- **Entry Point:** Ensure `vertebra.server.worker` (Python) correctly attaches to the FDs provided by the new `WorkerManager`. (This might be existing logic, verify and adapt if needed).

## API Changes

### Rust (`vertebra-server`)
```rust
pub struct WorkerManager { ... }

impl WorkerManager {
    pub fn new(workers: usize, app_path: &str) -> Self;
    pub fn start(&mut self) -> Result<()>;
    pub fn acquire_ring(&self) -> (Arc<SpscSender>, Arc<SpscReceiver>); // For HTTP handling loop
}
```
