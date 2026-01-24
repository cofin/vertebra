# Phase 5: Single-File Distribution & Supervisor

**Goal**: Zero-dependency deployment via `pyapp` + Rust Supervisor.
**Targets**: Linux + macOS for initial release; Windows later.

## Tasks

### Rust Supervisor (vertebra-py)
- [ ] **P5.1**: Add `vertebra-broker` as a dependency to `vertebra-py` (library crate).
- [ ] **P5.2**: Implement `supervisor.rs` in `vertebra-py`:
    - Start Broker in background thread.
    - Spawn Bun binary (subprocess).
    - Handle Shutdown (Signal hooking).
- [ ] **P5.3**: Expose `init_supervisor()` PyO3 function.

### Engine Compilation
- [ ] **P5.4**: Create `scripts/build_engine.sh` (Bun compile).
- [ ] **P5.5**: Update `pyproject.toml` artifacts to include `bin/`.

### Build Tooling
- [ ] **P5.6**: Implement `vertebra build --one-file`.
- [ ] **P5.7**: Configure `pyapp` in `pyproject.toml`.

### CI/CD
- [ ] **P5.8**: Release Workflow: Compile Bun -> Build Rust/Wheel -> Bundle PyApp.

## Verification
- Build single file.
- Run on clean machine.
- Verify Broker is active (internal thread).
- Verify Bun is running (subprocess).
- Verify Python is serving.
