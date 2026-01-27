# Spec: Frontend Native Bridge

## Code Analysis Summary
**Current State:**
- `src/ts/src/codegen/vite-plugin.ts`: Vite plugin for fetching types from the Python server (via HTTP).
- `src/py/vertebra/cli/build.py`: Python CLI for building, currently focuses on the server binary.
- **Goal:** Native integration for frontend assets. The Rust server should be able to serve static assets directly and potentially manage the dev server process.

**Target Architecture:**
- **Rspack Integration:** We want "native" bundling. Rspack is Rust-based and fast.
- **Dev Mode:**
    - `vertebra serve --dev` should start the Rspack dev server (or Vite) and proxy requests to it.
    - The Rust server acts as the primary entry point (`http://localhost:8000`), proxying `/` to the frontend dev server and handling `/api` itself.
- **Prod Mode:**
    - `vertebra build` should trigger the frontend build (Rspack/Vite).
    - The Rust server serves the built static files from `dist/`.

## Requirements

### 1. Asset Serving (Rust)
- **Crate:** `vertebra-server`
- **Functionality:**
    - `serve_static(dir: Path)`: Efficiently serve files.
    - `proxy_to(url: Url)`: Proxy requests to a dev server.

### 2. Frontend Build CLI
- **Command:** `vertebra build` enhancement.
- **Logic:**
    - Check for `package.json`.
    - Detect builder (Rspack vs Vite).
    - Run build command (`npm run build`).
    - Collect artifacts into a distribution folder.

### 3. Dev Server Management
- **Command:** `vertebra serve --dev`
- **Logic:**
    - Spawn frontend dev server as a child process.
    - Wait for it to be ready.
    - Proxy traffic.

## API Changes

### Rust (`vertebra-server`)
```rust
pub struct AssetManager { ... }
impl AssetManager {
    pub fn serve(&self, req: Request) -> Response; // Static or Proxy
}
```
