# Spec: Python ASGI Adapter

## Code Analysis Summary
**Current State:**
- `src/py/vertebra/core/worker.py`: Contains `AsgiWorker`. It currently *creates* the control ring (`AsyncRing.create_named`).
- **Mismatch:** In V2, the **Rust Server** (`vertebra-server`) creates the rings and spawns the worker. The worker should *attach* to them.

**Target Architecture:**
- **Rust Server**: Creates SHM regions and Rings. Sets `VERTEBRA_SHM_NAME` (or passes FDs).
- **Python Worker**:
    - Reads env vars.
    - **Attaches** to existing Rings (via `AsyncRing.open_named` or `from_fd`).
    - enters `serve_loop`.
    - Reads `RequestHeader` (struct) + Body from Ring.
    - Constructs ASGI Scope.
    - Invokes App.
    - Writes Response to Response Ring.

## Requirements

### 1. Update `AsgiWorker` Startup
- **Modify `start()`**:
    - Check for `VERTEBRA_WORKER_MODE=server_spawned` (or similar signal).
    - If spawned by server:
        - Do NOT create rings.
        - Open rings using provided names/FDs.
        - `AsyncRing.open_named` or `AsyncRing.from_raw_fd_handle`.

### 2. Protocol Alignment
- Ensure `worker.py`'s `_handle_control_payload` and `_handle_client_payload` match the binary format sent by `vertebra-server`.
- **Format:** The `RequestHeader` (16-byte aligned) + MsgPack metadata + Body.
    - `worker.py` currently handles `RequestHeader` + MsgPack.
    - Ensure `vertebra-server` sends exactly this.

### 3. Loop Optimization
- Verify `_serve_loop` uses the `notify_fd` passed by Rust correctly (it seems to already support this via `notify_read_fd` config).

## API Changes

### Python (`vertebra.core.worker`)
- Update `WorkerConfig` to accept ring names/paths if not using fixed convention.
- Logic change in `start()` to attach instead of create.
