# Flow Spec: Project Buildout
## Goal
Implement the high-level Python application shell, CLI, banners, and worker lifecycle management.

## Context
Vertebra requires a developer-friendly entrypoint. The CLI handles starting the Rust server/broker, which in turn spawns Python worker processes. This PRD covers the "outer shell" of the application.

## Plan
### Phase 1: CLI and Configuration
- [ ] Task 1: Implement `src/py/vertebra/cli/`. Add standard CLI commands (`run`, `build`, `init`) using `argparse` or a standard library equivalent.
- [ ] Task 2: Implement `src/py/vertebra/cli/banner/` to render the startup ASCII banner.
- [ ] Task 3: Create the `VertebraApp` class in `src/py/vertebra/web/app.py` to serve as the user's primary application interface.

### Phase 2: Supervisor & Worker Shell
- [ ] Task 4: Implement `src/py/vertebra/core/supervisor.py` to handle spawning the Rust core (which will manage workers).
- [ ] Task 5: Implement `src/py/vertebra/core/runner.py`. This will house the core execution loop that polls `ffi.get_batch()`. Leave the FFI calls stubbed out for now.
