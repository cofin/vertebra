# Flow Spec: Vectorized FFI API
## Goal
Expose batch polling and submission endpoints to Python.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Polling API
- [ ] Task 0: Open GitHub issue for "Vectorized FFI Polling and Submission API".
- [ ] Task 1: Implement `src/rs/vertebra-py/src/api.rs`. Add `get_batch()` PyO3 function.
- [ ] Task 2: Handle `BatchNode` extraction from the IPC ring.

### Phase 2: Submission API
- [ ] Task 3: Implement `submit_results()` PyO3 function.
- [ ] Task 4: Implement `src/rs/vertebra-py/src/response.rs` to convert Python returns to SHM responses.
