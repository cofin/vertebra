# Flow Spec: VertebraApp Python API
## Goal
Create the minimal application surface for users to define routes.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Core App Class
- [ ] Task 0: Open GitHub issue for "VertebraApp Application API".
- [ ] Task 1: Implement `src/py/vertebra/web/app.py` with `VertebraApp`.
- [ ] Task 2: Add route registration storage (mapping paths to Handler IDs).
- [ ] Task 3: Implement `.mount(path, app)` for WSGI/ASGI sub-apps.

### Phase 2: Decorators
- [ ] Task 4: Implement `src/py/vertebra/web/routing.py`. Add `@app.get`, `@app.post`, etc.
- [ ] Task 5: Implement Handler ID generation (stable within a session).
