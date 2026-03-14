# Flow Spec: ASGI Core Adapter
## Goal
Implement ASGI compatibility over native batch.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Scope & Queues
- [ ] Task 0: Open GitHub issue for "ASGI Core Adapter and Scope mapping".
- [ ] Task 1: Implement `src/py/vertebra/asgi/scope.py` builder.
- [ ] Task 2: Implement `src/py/vertebra/asgi/queues.py` for async signaling.

### Phase 2: Async Runner
- [ ] Task 3: Implement `src/py/vertebra/asgi/adapter.py`.
- [ ] Task 4: Set up `asyncio.TaskGroup` for batch task execution.
