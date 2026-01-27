# Plan: Frontend Native Bridge

## Phase 1: Rust Asset Serving
- [~] **Task: Implement `AssetManager` in `vertebra-server`**
    - Add `tower-http` (ServeDir) dependency.
    - Implement static file serving for production mode.
    - Implement a basic HTTP proxy for dev mode (using `hyper` client).

## Phase 2: CLI Build Integration
- [ ] **Task: Update `vertebra build`**
    - In `src/py/vertebra/cli/build.py`:
    - Add logic to detect `package.json`.
    - If found, run `npm install` and `npm run build`.
    - Ensure output goes to a standard location (e.g., `dist/public`).

## Phase 3: Dev Server Orchestration
- [ ] **Task: Update `NativeSupervisor`**
    - Add ability to spawn a frontend dev command (e.g., `npm run dev`).
    - Pass the dev server port to the Rust server so it knows where to proxy.

## Phase 4: Verification
- [ ] **Task: End-to-End Test**
    - Create a test project with a simple index.html (or React app).
    - Run `vertebra build`.
    - Run `vertebra serve`.
    - Verify `localhost:8000` serves the index.html.
