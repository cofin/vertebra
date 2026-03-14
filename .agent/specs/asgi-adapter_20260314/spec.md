# Flow Spec: ASGI Adapter
## Goal
Implement ASGI compatibility over the Native Batch architecture.

## Context
Support for async Python frameworks (FastAPI, standard Litestar) requires an ASGI adapter. Because ASGI is heavily async and dict-based, this adapter inflates the `VertebraRequest` into a standard ASGI `scope` and drives the app via `asyncio`.

## Plan
### Phase 1: ASGI Emulation
- [ ] Task 1: Implement `src/py/vertebra/asgi/adapter.py`.
- [ ] Task 2: Create the `scope` builder that translates a `VertebraRequest` proxy into the ASGI spec dictionaries.
- [ ] Task 3: Implement `receive` and `send` async queues/holders to interface with the batch loop. 

### Phase 2: Async Runner
- [ ] Task 4: Implement an async event loop integration that takes a batch of ASGI requests, spawns tasks for them, and collects results back into the native `submit_batch` system.
- [ ] Task 5: Add ASGI support to `VertebraApp.mount()`.
