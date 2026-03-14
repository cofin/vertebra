# Flow Spec: Batch Collector
## Goal
Aggregate incoming HTTP requests into stable batch arenas.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Collection Loop
- [ ] Task 0: Open GitHub issue for "Request Batch Collection and Policy".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/collector.rs`.
- [ ] Task 2: Implement batch drainage based on size (e.g., 64 reqs) or timeout (e.g., 1ms).

### Phase 2: Policy
- [ ] Task 3: Implement `src/rs/vertebra-http/src/policy.rs` for adaptive batch sizing.
