# Flow Spec: Frontend Integration
## Goal
Integrate the native bundler (Rolldown), Vite compatibility, and EMR/Oxtools workflows from the Litecore/Vertebra history.

## Context
The goal is to port the advanced Type Bridge and Native Bundler features learned from Litecore. This allows Python types and routes to seamlessly inform frontend builds and provide HMR (Hot Module Replacement).

## Plan
### Phase 1: Build Orchestrator
- [ ] Task 1: Port `build_orchestrator.py` and related bundler plugins into `src/py/vertebra/core/build/`.
- [ ] Task 2: Ensure `pyproject.toml` is updated with necessary frontend build tools (e.g., optional dependencies for Oxtools, if applicable).

### Phase 2: Type Bridge & HMR
- [ ] Task 3: Port the `TypeBridge` (IR generation, TypeScript emission) into `src/py/vertebra/core/codegen/`.
- [ ] Task 4: Implement HMR signaling via the Native HTTP server (using websockets directly from Rust to notify the frontend when Python types change).
