# Flow Spec: Python Batch Runner
## Goal
Implement the optimized worker execution loop.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Execution Loop
- [ ] Task 0: Open GitHub issue for "Python Batch Execution Loop".
- [ ] Task 1: Implement `src/py/vertebra/core/runner.py`.
- [ ] Task 2: Add the `while True` loop that invokes `ffi.get_batch()`.

### Phase 2: Worker Shell
- [ ] Task 3: Implement `src/py/vertebra/core/worker.py` for worker initialization.
- [ ] Task 4: Add signal handling for clean exit.
