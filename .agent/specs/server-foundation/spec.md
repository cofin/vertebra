# Spec: Server Foundation

## Code Analysis Summary
**Current State:**
- `src/rs/vertebra-js`: Legacy Bun bindings (Scheduled for deletion).
- `src/rs/vertebra-py`: Existing PyO3 bindings (IPC only).
- `vertebra-core`: Shared types.

**Target Architecture:**
- **New Crate:** `src/rs/vertebra-server`
    - Dependencies: `hyper`, `tokio`, `tower`, `http-body-util`.
    - Function: Standalone HTTP server library.
- **Modified Crate:** `src/rs/vertebra-py`
    - Dependency: `vertebra-server`.
    - New Binding: `start_server(config: ServerConfig)`.
- **Python:** `vertebra.server.start_server` wrapper.

## Requirements

### 1. Rust Server Crate
- **Crate Name:** `vertebra-server`.
- **Functionality:**
    - Initialize a Multi-Threaded Tokio Runtime.
    - Bind to a TCP port (default 8000).
    - Serve a basic "Hello from Vertebra Rust" plaintext response (for validation).
    - Handle `SIGINT` / `SIGTERM` for graceful shutdown.

### 2. Python Bindings
- **Function:** `vertebra._vertebra.start_http_server(host: str, port: int) -> None`.
- **Behavior:**
    - Blocking call (blocks the Python thread).
    - Starts the Tokio runtime.
    - Runs the Hyper server.
    - Returns on shutdown.

### 3. Cleanup
- **Remove:** `src/rs/vertebra-js` (folder and workspace member).

## API Changes

### Rust (`vertebra-py/lib.rs`)
```rust
#[pyfunction]
fn start_http_server(py: Python, host: &str, port: u16) -> PyResult<()> {
    // ...
}
```

### Python
```python
# src/py/vertebra/server.py
def run(host="127.0.0.1", port=8000):
    _vertebra.start_http_server(host, port)
```
