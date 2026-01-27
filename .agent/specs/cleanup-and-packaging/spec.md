# Spec: Cleanup and Packaging

## Code Analysis Summary
**Current State:**
- `src/ts`: Contains the legacy Bun broker and server logic.
- `src/rs/vertebra-js`: Rust bindings for the legacy Bun code.
- `src/rs/Cargo.toml`: References `vertebra-js`.
- `vertebra build`: Still points to `build-engine.ts` (Bun build script).

**Target Architecture:**
- **Removal:** `src/ts` and `src/rs/vertebra-js` are deleted.
- **Build System:** `vertebra build` now compiles `vertebra-server` (Rust) using `cargo build --release`.
- **PyApp:** The final binary includes the Rust server executable and the Python runtime.

## Requirements

### 1. Code Removal
- Delete `src/ts` directory.
- Delete `src/rs/vertebra-js` directory.
- Remove `vertebra-js` from `src/rs/Cargo.toml` workspace members.

### 2. Build Command Update
- **Modify `src/py/vertebra/cli/build.py` (and underlying bundler):**
    - Remove calls to `bun build`.
    - Add calls to `cargo build --release --bin vertebra-server`.
    - Copy the resulting binary (`target/release/vertebra-server`) to the distribution folder.

### 3. Documentation Update
- Update `README.md` to reflect the new Rust-centric architecture.
- Remove references to Bun installation requirements (except for frontend dev).

## API Changes
- None (Internal cleanup).
