# Flow Spec: Native Routing & Dispatch
## Goal
Implement Rust-side URL routing and route-id dispatching.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Routing
- [ ] Task 0: Open GitHub issue for "Native Rust Router and Handler ID resolution".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/router.rs` using `matchit`.
- [ ] Task 2: Map HTTP paths/methods to stable `Handler ID` (uint64).

### Phase 2: Dispatch
- [ ] Task 3: Implement `src/rs/vertebra-http/src/dispatcher.rs`.
- [ ] Task 4: Push routed requests into the IPC `MPMC` ring buffer.
