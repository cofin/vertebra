# Flow Spec: WSGI Core Adapter
## Goal
Implement high-performance WSGI emulation over native batch.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: WSGI Environ
- [ ] Task 0: Open GitHub issue for "WSGI Core Adapter and Lazy Environ".
- [ ] Task 1: Implement `src/py/vertebra/wsgi/environ.py`.
- [ ] Task 2: Create `LazyWsgiEnviron` wrapper for `VertebraRequest`.

### Phase 2: WSGI Runner
- [ ] Task 3: Implement `src/py/vertebra/wsgi/adapter.py`.
- [ ] Task 4: Implement the `start_response` callback and body iterable collection.
