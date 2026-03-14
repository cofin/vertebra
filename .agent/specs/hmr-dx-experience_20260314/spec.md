# Flow Spec: HMR & Developer Experience
## Goal
Rust-powered websocket HMR signaling.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Rust Websockets
- [ ] Task 0: Open GitHub issue for "Rust-powered HMR signaling and Developer Experience".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/hmr.rs`. Add lightweight WS server.
- [ ] Task 2: Add signaling API for Python to trigger HMR updates.

### Phase 2: Watcher
- [ ] Task 3: Implement `src/py/vertebra/core/codegen/watch.py` using `watchfiles`.
- [ ] Task 4: Integrate watcher into the CLI `run --dev` command.
