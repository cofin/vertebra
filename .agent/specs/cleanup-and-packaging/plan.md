# Plan: Cleanup and Packaging

## Phase 1: Code Removal
- [ ] **Task: Delete Legacy Code**
    - `rm -rf src/ts`
    - `rm -rf src/rs/vertebra-js`
- [ ] **Task: Update Workspace**
    - Remove `vertebra-js` from `src/rs/Cargo.toml`.

## Phase 2: Build System Update
- [ ] **Task: Update Bundler**
    - In `src/py/vertebra/core/build/bundler.py`:
    - Replace `build_engine_bun` logic with `build_server_rust`.
    - `build_server_rust` should run `cargo build --release -p vertebra-server`.

## Phase 3: Final Verification
- [ ] **Task: Full Build Test**
    - Run `vertebra build --onefile`.
    - Verify the output binary runs and starts the HTTP server.
- [ ] **Task: Clean Install Test**
    - Create a fresh venv.
    - `pip install dist/vertebra-...whl`.
    - `vertebra serve`.
