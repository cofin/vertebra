# Flow Spec: WSGI Adapter
## Goal
Implement a high-performance WSGI compatibility layer on top of the Native Batch architecture.

## Context
While the "Ghost Framework" native mode is the default, we must support WSGI applications (like Django/Flask) with high performance. We will emulate the WSGI `environ` dictionary lazily based on the `VertebraRequest` proxy.

## Plan
### Phase 1: WSGI Emulation
- [ ] Task 1: Implement `src/py/vertebra/asgi/wsgi_adapter.py`. (Despite the folder name, this is the legacy protocols module).
- [ ] Task 2: Create a `LazyWsgiEnviron` class that wraps `VertebraRequest` and mimics a dict to avoid allocating standard WSGI keys unless accessed.
- [ ] Task 3: Implement the synchronous WSGI runner loop inside `wsgi_adapter.py` that translates the WSGI response iterable back into a `VertebraResponse` payload.

### Phase 2: Extensions Integration
- [ ] Task 4: Add WSGI support to `VertebraApp`'s mount/routing API (`app.mount("/wsgi", wsgi_app)`).
