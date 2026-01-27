# Plan: Server Foundation

## Phase 1: Cleanup
- [~] **Task: Remove Legacy JS Bindings**
    - `rm -rf src/rs/vertebra-js`
    - Remove `vertebra-js` from `src/rs/Cargo.toml` workspace members.

## Phase 2: Rust Server Crate
- [ ] **Task: Scaffold `vertebra-server`**
    - Create `src/rs/vertebra-server/Cargo.toml`.
    - Add deps: `tokio` (full), `hyper` (full), `http-body-util`, `hyper-util`.
    - Add to workspace `src/rs/Cargo.toml`.
- [ ] **Task: Implement Basic Hyper Server**
    - `src/rs/vertebra-server/src/lib.rs`: `pub async fn serve(host: &str, port: u16)`.
    - Implement `service_fn` returning "Hello from Vertebra".
    - Implement `tokio::signal::ctrl_c` handler.

## Phase 3: Python Bindings
- [ ] **Task: Link `vertebra-server` to `vertebra-py`**
    - Update `src/rs/vertebra-py/Cargo.toml`: Add `vertebra-server = { path = "../vertebra-server" }`.
- [ ] **Task: Expose `start_http_server`**
    - `src/rs/vertebra-py/src/lib.rs`: Add `start_http_server`.
    - Initialize Tokio runtime inside the PyFunction.
    - Block on `runtime.block_on(server.serve(...))`.

## Phase 4: Validation
- [ ] **Task: Python Verification Script**
    - Create `tools/test_server_boot.py`.
    - Call `_vertebra.start_http_server("127.0.0.1", 3000)`.
    - Validate with `curl http://127.0.0.1:3000`.
