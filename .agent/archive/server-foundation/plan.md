# Plan: Server Foundation

## Phase 1: Cleanup
- [x] **Task: Remove Legacy JS Bindings** 9493217
    - `rm -rf src/rs/vertebra-js`
    - Remove `vertebra-js` from `src/rs/Cargo.toml` workspace members.

## Phase 2: Rust Server Crate
- [x] **Task: Scaffold `vertebra-server`** 9b2a637
    - Create `src/rs/vertebra-server/Cargo.toml`.
    - Add deps: `tokio` (full), `hyper` (full), `http-body-util`, `hyper-util`.
    - Add to workspace `src/rs/Cargo.toml`.
- [x] **Task: Implement Basic Hyper Server** 3ad0c76
    - `src/rs/vertebra-server/src/lib.rs`: `pub async fn serve(host: &str, port: u16)`.
    - Implement `service_fn` returning "Hello from Vertebra".
    - Implement `tokio::signal::ctrl_c` handler.

## Phase 3: Python Bindings
- [x] **Task: Link `vertebra-server` to `vertebra-py`** e9f412c
    - Update `src/rs/vertebra-py/Cargo.toml`: Add `vertebra-server = { path = "../vertebra-server" }`.
- [x] **Task: Expose `start_http_server`** 222678a
    - `src/rs/vertebra-py/src/lib.rs`: Add `start_http_server`.
    - Initialize Tokio runtime inside the PyFunction.
    - Block on `runtime.block_on(server.serve(...))`.

## Phase 4: Validation
- [x] **Task: Python Verification Script** 31a9e21
    - Create `tools/test_server_boot.py`.
    - Call `_vertebra.start_http_server("127.0.0.1", 3000)`.
    - Validate with `curl http://127.0.0.1:3000`.

## Phase 5: Completion
- [x] **Task: Run Test Suite & Checkpoint**
    - `make test-all`
    - Create tag `checkpoint/server-foundation/phase-4` (2f318dc)
- [x] **Task: Archive Flow**
    - Move to `.agent/archive/`
