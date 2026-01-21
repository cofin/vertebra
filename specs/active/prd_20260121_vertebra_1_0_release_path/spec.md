# Vertebra 1.0 Release Path

## Overview
This PRD consolidates the final critical path to the Vertebra 1.0 release. It focuses on stabilizing the core data plane (SQLSpec/Arrow), implementing the complete distribution toolchain (specifically the User Application Distribution strategy), and delivering a full-featured MCP (Model Context Protocol) runtime.

**Key constraints:**
- SQLAlchemy and Advanced Alchemy integrations are explicitly **out of scope**.
- "Bug squashing" is a primary deliverable.
- **User Application Distribution** is a primary goal: Users must be able to build standalone, single-file executables of their applications using `vertebra build`.
- **Framework Distribution** (PyPI) is separate and follows standard wheel patterns.

## Research Reference
- `specs/archive/build-distribution-experience/prd.md` (Source of Truth for Distribution Strategy)

## Functional Requirements

### 1. Data Plane Stabilization (SQLSpec + Arrow)
#### Must Have
- [x] **Arrow IPC Integration:** Automatic switch to Arrow serialization for large result sets (> threshold).
- [x] **Msgspec Codec Hardening:** Robust encoding/decoding for all core Python types (Date, UUID, Enum, etc.).
- [x] **Shared Memory Safety:** Zero-leak guarantee for ring buffers.

### 2. Distribution & Packaging (Aligned with PRD-9)
#### Must Have
- [ ] **User Application Distribution:** `vertebra build --one-file` must enable users to package their *own* applications into standalone binaries.
- [ ] **Standalone Runtime Management:**
    -   Automatic download and caching of `python-build-standalone` releases.
    -   Support for optimization profiles: `modern` (x86-64-v3), `baseline` (x86-64), and `native`.
    -   Offline capability: Once cached, builds must work without network access.
- [ ] **Unified Build Orchestrator:**
    -   Bundles: Python Runtime (PBS) + Bun Engine + User App + Dependencies + Site-Packages.
    -   Resolves dependencies offline.
- [ ] **Rust Supervisor:** Robust process management (GIL-release aware) for spawning Bun and Python workers.
- [ ] **Signing Documentation:** Comprehensive guide for macOS (notarization) and Windows (code signing).

### 3. Server Runtime & MCP
#### Must Have
- [ ] **MCP Stdio Transport:** For CLI and Claude Desktop integration.
- [ ] **MCP SSE/WebSocket Transports:** For web clients and agents.
- [ ] **Python Decorator API:** `@mcp_tool` and `@mcp_resource`.
- [ ] **Schema Generation:** Automatic JSON schema from type hints.

## Technical Approach

### Distribution Strategy (Refined)
The "Distribution" phase is split into two distinct workflows:

1.  **User Application Distribution (`vertebra build`):**
    -   **Tooling:** Uses `python-build-standalone` (PBS) to provide a portable Python runtime.
    -   **Mechanism:** `vertebra.build.standalone` module manages PBS downloads/caching.
    -   **Bundling:** `vertebra.build.bundler` orchestrates the creation of the final artifact:
        -   Extracts PBS.
        -   Installs user dependencies into the bundled site-packages.
        -   Copies the correct `vertebra-engine` (Bun binary).
        -   Bundles user code.
    -   **Profiles:**
        -   `modern`: Best performance (recent CPUs).
        -   `baseline`: Max compatibility (older CPUs).
        -   `native`: Current system python (dev/testing).

2.  **Framework Distribution (PyPI):**
    -   Standard `cibuildwheel` process to publish `vertebra` wheels to PyPI.
    -   (Out of scope for this immediate coding task, but contextually important).

### Libraries/APIs
- **Python:** `httpx` (for PBS downloads), `tarfile`/`zipfile` (extraction), `click` (CLI).
- **Rust:** `pyo3` (bindings), `signal-hook` (lifecycle).

### Files to Modify
- `src/py/vertebra/build/standalone.py`: Manages `python-build-standalone` versions and downloads.
- `src/py/vertebra/build/bundler.py`: Core logic for assembling the single-file executable.
- `src/py/vertebra/build/manager.py`: High-level build coordination.
- `src/py/vertebra/cli.py`: Expose `vertebra build` commands.

## Acceptance Criteria
- [ ] **Standalone Build:** `vertebra build --one-file` produces a binary that runs on a fresh machine (Docker container) without Python installed.
- [ ] **Profile Support:** Can build both `modern` and `baseline` targets.
- [ ] **Offline Mode:** Build works if PBS is already cached, even with network disabled.
- [ ] **MCP:** Built binary successfully serves MCP tools.

## Risk Assessment
| Risk | Mitigation |
|------|------------|
| **PBS Download Flakiness** | Implement robust caching and checksum verification. Use `httpx` with retries. |
| **Binary Size** | Acceptable (~80-150MB). Warn user but prioritize UX/Simplicity. |
| **Dependency Resolution** | Rely on `uv` or `pip` to resolve deps into the bundle target directory. |

## Safe Checkpoints
- **Checkpoint 1:** Phase 1 Complete (Done).
- **Checkpoint 2:** `vertebra.build.standalone` implemented and testing downloading/caching.
- **Checkpoint 3:** `vertebra.build.bundler` implemented and producing artifacts.