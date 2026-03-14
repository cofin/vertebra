# Flow Spec: Rust Supervisor & Worker Lifecycle
## Goal
Implement the Rust-side process manager that spawns and manages Python workers.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Process Spawning
- [ ] Task 0: Open GitHub issue for "Rust Supervisor and Worker Process Management".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/supervisor.rs` using `std::process::Command`.
- [ ] Task 2: Handle worker environment variable injection (SHM FDs, etc.).

### Phase 2: Management
- [ ] Task 3: Implement health checks and auto-restart for crashed workers.
- [ ] Task 4: Implement graceful shutdown signaling (SIGTERM handling).
